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
#include <vector>

HTTPDirectory::HTTPDirectory(XrdSysError &log, HTTPFileSystem *oss)
	: m_log(log), m_oss(oss), m_bytesReturned(0) {} // Initialize it to false.

std::map<std::string, struct stat>
HTTPDirectory::parseHTMLToFSSpecString(const std::string &htmlContent) {
	using namespace tinyxml2;
	std::map<std::string, struct stat> remoteList;

	XMLDocument doc;
	XMLError error = doc.Parse(htmlContent.c_str());
	if (error != XML_SUCCESS) {
		std::cerr << "Failed to parse HTML!" << std::endl;
		return remoteList;
	}

	// Root of the HTML document
	XMLNode *root = doc.FirstChild();
	if (!root) {
		std::cerr << "No root found in HTML!" << std::endl;
		return remoteList;
	}

	// Traverse the rows in the table
	for (XMLElement *row = root->FirstChildElement("tr"); row != nullptr;
		 row = row->NextSiblingElement("tr")) {
		FSSpecEntry entry;
		int columnIndex = 0;

		// Traverse each cell in the row
		for (XMLElement *cell = row->FirstChildElement("td"); cell != nullptr;
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
				if (XMLElement *aTag = cell->FirstChildElement("a")) {
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
		remoteList[entry.name] = workingFile;
	}

	return remoteList; // Return the formatted list
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

int HTTPDirectory::Opendir(const char *path, XrdOucEnv &env) {
	m_log.Log(LogMask::Debug, "HTTPDirectory::Opendir", "Opendir called");
	auto configured_hostname = m_oss->getHTTPHostName();
	auto configured_hostUrl = m_oss->getHTTPHostUrl();
	const auto &configured_url_base = m_oss->getHTTPUrlBase();
	if (!configured_url_base.empty()) {
		configured_hostUrl = configured_url_base;
		configured_hostname = m_oss->getStoragePrefix();
	}

	//
	// Check the path for validity.
	//
	std::string object;
	int rv = parse_path(configured_hostname, path, object);

	if (rv != 0) {
		return rv;
	}

	m_object = object;
	m_hostname = configured_hostname;
	m_hostUrl = configured_hostUrl;
	m_remote_flavor = m_oss->getRemoteFlavor();

	if (m_remoteList.empty()) {
		m_log.Log(LogMask::Debug, "HTTPFile::Opendir", "Opendir called");
		HTTPList list(m_hostUrl, m_object, m_log, m_oss->getToken());
		m_log.Log(LogMask::Debug, "HTTPDirectory::Opendir",
				  "About to perform download from HTTPDirectory::Opendir(): "
				  "hostname / object:",
				  m_hostname.c_str(), m_object.c_str());
		if (!list.SendRequest()) {
			std::stringstream ss;
			ss << "Failed to send GetObject command: " << list.getResponseCode()
			   << "'" << list.getResultString() << "'";
			m_log.Log(LogMask::Warning, "HTTPDirectory::Opendir",
					  ss.str().c_str());
			return 0;
		}

		m_remoteList =
			parseHTMLToFSSpecString(extractHTMLTable(list.getResultString()));
	}

	return 0;
}
