/***************************************************************
 *
 * Copyright (C) 2024, Pelican Project, Morgridge Institute for Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

#include "HTTPDirectory.hh"
#include "HTTPCommands.hh"
#include "HTTPFile.hh"
#include "HTTPFileSystem.hh"
#include "logging.hh"
#include "stl_string_utils.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdVersion.hh>

#include <cstring>
#include <curl/curl.h>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <regex>
#include <sstream>
#include <string>
#include <tinyxml2.h>

using namespace XrdHTTPServer;

HTTPDirectory::HTTPDirectory(XrdSysError &log, HTTPFileSystem &oss)
	: m_log(log), m_oss(oss) {} // Initialize it to false.

void HTTPDirectory::parseHTMLToListing(const std::string &htmlContent) {
	m_remoteList.clear();

	tinyxml2::XMLDocument doc;
	tinyxml2::XMLError error = doc.Parse(htmlContent.c_str());
	if (error != tinyxml2::XML_SUCCESS) {
		m_log.Log(LogMask::Warning, "HTTPDirectory",
				  "Failed to parse HTML in directory response");
		return;
	}

	// Root of the HTML document
	auto root = doc.FirstChild();
	if (!root) {
		m_log.Log(LogMask::Warning, "HTTPDirectory",
				  "No root found in HTML in directory response");
		return;
	}

	// Traverse the rows in the table
	for (auto row = root->FirstChildElement("tr"); row != nullptr;
		 row = row->NextSiblingElement("tr")) {
		Entry entry;
		int columnIndex = 0;

		// Traverse each cell in the row
		for (auto cell = row->FirstChildElement("td"); cell != nullptr;
			 cell = cell->NextSiblingElement("td")) {
			const char *cellText = cell->GetText() ? cell->GetText() : "";

			switch (columnIndex) {
			case 0: // Mode
				entry.mode = cellText;
				break;
			case 1: // Flags
				entry.flags = cellText;
				break;
			case 2: // Size
				entry.size = cellText;
				break;
			case 3: // Modified
				entry.modified = cellText;
				break;
			case 4: // Name
				if (auto aTag = cell->FirstChildElement("a")) {
					const char *nameText =
						aTag->GetText() ? aTag->GetText() : "";
					entry.name = nameText;
				}
				break;
			default:
				break;
			}
			columnIndex++;
		}

		// XRootD's HTML index appends a trailing '/' to directory names; drop
		// it so entry names match those returned by the WebDAV listing and by
		// POSIX readdir.
		if (!entry.name.empty() && entry.name.back() == '/') {
			entry.name.pop_back();
		}

		// Skip adding invalid/empty rows
		if (entry.name.empty()) {
			continue;
		}

		struct stat workingFile;
		workingFile.st_size = std::stoul(entry.size, nullptr, 10);
		// workingFile.st_mtime = std::stoul(entry.modified, nullptr, 10);
		if (entry.mode.substr(0, 1) == "d")
			workingFile.st_mode = 0600 | S_IFDIR;
		else
			workingFile.st_mode = 0600 | S_IFREG;

		workingFile.st_nlink = 1;
		workingFile.st_uid = 1;
		workingFile.st_gid = 1;
		workingFile.st_atime = 0;
		workingFile.st_ctime = 0;
		workingFile.st_dev = 0;
		workingFile.st_ino = 0;
		m_remoteList.push_back({entry.name, workingFile});
	}
}

namespace {

// tinyxml2 is namespace-unaware, so element names arrive with their XML
// prefix attached (e.g. "D:response", "lp1:resourcetype").  Return the local
// name with any "prefix:" stripped.
std::string localName(const char *name) {
	std::string s(name ? name : "");
	auto pos = s.rfind(':');
	return pos == std::string::npos ? s : s.substr(pos + 1);
}

// Depth-first search for the first descendant element with the given local
// name.  Does not consider `root` itself.
const tinyxml2::XMLElement *findByLocalName(const tinyxml2::XMLElement *root,
											const std::string &ln) {
	if (!root) {
		return nullptr;
	}
	for (auto child = root->FirstChildElement(); child != nullptr;
		 child = child->NextSiblingElement()) {
		if (localName(child->Name()) == ln) {
			return child;
		}
		if (auto found = findByLocalName(child, ln)) {
			return found;
		}
	}
	return nullptr;
}

// Collect every descendant element with the given local name.  A matched
// element is not descended into (WebDAV <response> elements are never nested).
void collectByLocalName(const tinyxml2::XMLElement *root, const std::string &ln,
						std::vector<const tinyxml2::XMLElement *> &out) {
	if (!root) {
		return;
	}
	for (auto child = root->FirstChildElement(); child != nullptr;
		 child = child->NextSiblingElement()) {
		if (localName(child->Name()) == ln) {
			out.push_back(child);
		} else {
			collectByLocalName(child, ln, out);
		}
	}
}

// Strip leading and trailing '/' characters.
std::string trimSlashes(const std::string &s) {
	auto begin = s.find_first_not_of('/');
	if (begin == std::string::npos) {
		return "";
	}
	auto end = s.find_last_not_of('/');
	return s.substr(begin, end - begin + 1);
}

// Reduce a WebDAV href to a server-relative path with surrounding slashes
// removed.  Handles both absolute-path hrefs ("/testdir/file") and full-URL
// hrefs ("https://host/testdir/file").
std::string hrefToRelPath(const std::string &href) {
	std::string path = href;
	auto scheme = path.find("://");
	if (scheme != std::string::npos) {
		auto slash = path.find('/', scheme + 3);
		path = (slash == std::string::npos) ? "" : path.substr(slash);
	}
	return trimSlashes(path);
}

} // namespace

void HTTPDirectory::parseWebDAVToListing(const std::string &xmlContent,
										 const std::string &requestObject) {
	m_remoteList.clear();

	tinyxml2::XMLDocument doc;
	if (doc.Parse(xmlContent.c_str()) != tinyxml2::XML_SUCCESS) {
		m_log.Log(LogMask::Warning, "HTTPDirectory",
				  "Failed to parse WebDAV PROPFIND response");
		return;
	}

	const std::string base = trimSlashes(requestObject);

	std::vector<const tinyxml2::XMLElement *> responses;
	collectByLocalName(doc.RootElement(), "response", responses);

	for (auto response : responses) {
		auto hrefEl = findByLocalName(response, "href");
		if (!hrefEl || !hrefEl->GetText()) {
			continue;
		}
		const std::string relPath = hrefToRelPath(hrefEl->GetText());

		// Skip the entry describing the listed collection itself.
		if (relPath == base) {
			continue;
		}

		// The entry name is the final path component.
		auto slash = relPath.find_last_of('/');
		std::string name =
			(slash == std::string::npos) ? relPath : relPath.substr(slash + 1);
		if (name.empty()) {
			continue;
		}

		// A resource is a directory if it carries a <collection/> resourcetype
		// or an <iscollection> flag set to 1.
		bool isDir = findByLocalName(response, "collection") != nullptr;
		if (!isDir) {
			auto isColl = findByLocalName(response, "iscollection");
			if (isColl && isColl->GetText() &&
				std::string(isColl->GetText()) == "1") {
				isDir = true;
			}
		}

		off_t size = 0;
		if (auto lenEl = findByLocalName(response, "getcontentlength")) {
			if (lenEl->GetText()) {
				try {
					size = static_cast<off_t>(std::stoll(lenEl->GetText()));
				} catch (const std::exception &) {
					size = 0;
				}
			}
		}

		struct stat entry;
		memset(&entry, 0, sizeof(entry));
		entry.st_mode = isDir ? (0700 | S_IFDIR) : (0600 | S_IFREG);
		entry.st_size = isDir ? 4096 : size;
		entry.st_nlink = 1;
		entry.st_uid = 1;
		entry.st_gid = 1;
		m_remoteList.push_back({name, entry});
	}
}

std::string HTTPDirectory::extractHTMLTable(const std::string &htmlContent) {
	std::regex tableRegex(R"(<table[^>]*>[\s\S]*?</table>)",
						  std::regex_constants::icase);

	std::smatch match;
	if (std::regex_search(htmlContent, match, tableRegex)) {
		return match.str();
	}

	return ""; // Return an empty string if no table is found
}

int HTTPDirectory::Readdir(char *buff, int blen) {
	if (m_remoteList.size() > 0) {
		std::string name = m_remoteList.begin()->first;
		struct stat currentRecord = m_remoteList.begin()->second;
		mystat->st_size = currentRecord.st_size;
		mystat->st_mode = currentRecord.st_mode;
		mystat->st_nlink = currentRecord.st_nlink;
		mystat->st_uid = currentRecord.st_uid;
		mystat->st_gid = currentRecord.st_gid;
		mystat->st_atime = currentRecord.st_atime;
		mystat->st_ctime = currentRecord.st_ctime;
		mystat->st_dev = currentRecord.st_dev;
		mystat->st_ino = currentRecord.st_ino;
		memcpy(buff, name.c_str(), name.size() + 1);
		m_remoteList.erase(m_remoteList.begin());
		return name.size();
	} else {
		buff[0] = '\0';
		return 0;
	}
}

int HTTPDirectory::listViaHTTP(const std::string &hostUrl,
							   const std::string &object) {
	HTTPList list(hostUrl, object, m_log, m_oss.getToken());
	m_log.Log(LogMask::Debug, "HTTPDirectory::listViaHTTP",
			  "Requesting HTML directory listing for object:", object.c_str());
	if (!list.SendRequest()) {
		std::stringstream ss;
		ss << "Failed to send directory GET command: " << list.getResponseCode()
		   << " '" << list.getResultString() << "'";
		m_log.Log(LogMask::Warning, "HTTPDirectory::listViaHTTP",
				  ss.str().c_str());
		return 0;
	}

	parseHTMLToListing(extractHTMLTable(list.getResultString()));
	return 0;
}

int HTTPDirectory::listViaWebDAV(const std::string &hostUrl,
								 const std::string &object) {
	HTTPPropfind propfind(hostUrl, object, m_log, m_oss.getToken());
	m_log.Log(LogMask::Debug, "HTTPDirectory::listViaWebDAV",
			  "Requesting WebDAV PROPFIND listing for object:", object.c_str());
	if (!propfind.SendRequest("1")) {
		// A 405 means the server does not implement PROPFIND; signal the caller
		// so it can fall back to an HTML listing.
		if (propfind.getResponseCode() == 405) {
			m_log.Log(LogMask::Info, "HTTPDirectory::listViaWebDAV",
					  "Server rejected PROPFIND (405 Method Not Allowed)");
			return -ENOTSUP;
		}
		std::stringstream ss;
		ss << "Failed to send PROPFIND command: " << propfind.getResponseCode()
		   << " '" << propfind.getResultString() << "'";
		m_log.Log(LogMask::Warning, "HTTPDirectory::listViaWebDAV",
				  ss.str().c_str());
		return 0;
	}

	parseWebDAVToListing(propfind.getResultString(), object);
	return 0;
}

int HTTPDirectory::Opendir(const char *path, XrdOucEnv &env) {
	m_log.Log(LogMask::Debug, "HTTPDirectory::Opendir", "Opendir called");
	auto configured_hostUrl = m_oss.getHTTPHostUrl();
	const auto &configured_url_base = m_oss.getHTTPUrlBase();
	if (!configured_url_base.empty()) {
		configured_hostUrl = configured_url_base;
	}

	//
	// Check the path for validity.
	//
	std::string object;
	int rv = parse_path(m_oss.getHTTPHostName(), path, object);

	if (rv != 0) {
		return rv;
	}

	if (!m_remoteList.empty()) {
		return 0;
	}

	RemoteFlavor flavor = m_oss.getResolvedFlavor();
	if (flavor == RemoteFlavor::Http) {
		return listViaHTTP(configured_hostUrl, object);
	}

	// WebDAV, or "auto" that has not resolved yet: optimistically try PROPFIND
	// and, when the flavor is still unknown, fall back to an HTML listing if
	// the server does not support WebDAV.
	int listrv = listViaWebDAV(configured_hostUrl, object);
	if (listrv == -ENOTSUP && flavor == RemoteFlavor::Unknown) {
		m_log.Log(LogMask::Info, "HTTPDirectory::Opendir",
				  "Falling back to HTML directory listing");
		return listViaHTTP(configured_hostUrl, object);
	}
	return listrv;
}
