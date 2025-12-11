/***************************************************************
 *
 * Copyright (C) 2025, Pelican Project, Morgridge Institute for Research
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

#include "GlobusDirectory.hh"
#include "GlobusFileSystem.hh"
#include "HTTPCommands.hh"
#include "logging.hh"
#include "stl_string_utils.hh"

#include <XrdOss/XrdOssWrapper.hh>
#include <XrdSys/XrdSysError.hh>

#include <nlohmann/json.hpp>

#include <sstream>

using json = nlohmann::json;

time_t parseTimestamp(const std::string &last_modified) {
	if (!last_modified.empty()) {
		struct tm tm_time = {};
		if (strptime(last_modified.c_str(), "%Y-%m-%d %H:%M:%S", &tm_time) !=
			nullptr) {
			return mktime(&tm_time);
		}
	}
	return 0;
}

void GlobusDirectory::Reset() {
	m_opened = false;
	m_idx = 0;
	m_objInfo.clear();
	m_directories.clear();
	m_stat_buf = nullptr;
	m_object = "";
}

int GlobusDirectory::ListGlobusDir() {
	m_log.Log(XrdHTTPServer::Debug, "GlobusDirectory::ListGlobusDir",
			  "Listing directory:", m_object.c_str());

	auto token = m_fs.getTransferToken();
	if (!token) {
		m_log.Emsg("Listing", "Failed to get transfer token");
		return -ENOENT;
	}

	HTTPDownload listCommand(m_fs.getLsUrl(), m_object, m_log, token);
	if (!listCommand.SendRequest(0, 0)) {
		return HTTPRequest::HandleHTTPError(
			listCommand, m_log, "Globus directory listing", m_object.c_str());
	}

	std::string response = listCommand.getResultString();
	try {
		auto json = json::parse(response);

		if (json.contains("DATA") && json["DATA"].is_array()) {
			const auto &data = json["DATA"];
			for (const auto &item : data) {
				if (item.contains("name") && item.contains("size") &&
					item.contains("type")) {
					GlobusObjectInfo obj;
					obj.m_key = item["name"].get<std::string>();
					obj.m_size = item["size"].get<size_t>();

					if (item.contains("last_modified")) {
						obj.m_last_modified =
							item["last_modified"].get<std::string>();
					}

					if (item["type"].get<std::string>() == "file") {
						m_objInfo.push_back(obj);
					} else if (item["type"].get<std::string>() == "dir") {
						std::string dirName = obj.m_key;
						if (dirName.back() != '/') {
							dirName += "/";
						}
						obj.m_key = dirName;
						m_directories.push_back(obj);
					}
				}
			}
		}

	} catch (const json::exception &e) {
		m_log.Log(XrdHTTPServer::Warning, "GlobusDirectory::ListGlobusDir",
				  "Failed to parse JSON response:", e.what());
		return -EIO;
	}

	m_idx = 0;
	m_opened = true;

	return 0;
}

int GlobusDirectory::Opendir(const char *path, XrdOucEnv &env) {
	if (m_opened) {
		return -EBADF;
	}
	Reset();

	std::string realPath = path;
	if (realPath.back() != '/') {
		realPath = realPath + "/";
	}

	std::string storagePrefix = m_fs.getStoragePrefix();
	std::string object;

	if (realPath.find(storagePrefix) == 0) {
		object = realPath.substr(storagePrefix.length());
	} else {
		object = realPath;
	}

	if (!object.empty() && object[0] == '/') {
		object = object.substr(1);
	}

	m_object = object;

	return ListGlobusDir();
}

int GlobusDirectory::Readdir(char *buff, int blen) {
	if (!m_opened) {
		return -EBADF;
	}

	if (m_stat_buf) {
		memset(m_stat_buf, '\0', sizeof(struct stat));
	}

	// m_idx encodes the location inside the current directory.
	// - m_idx in [0, m_objInfo.size) means return a "file" from the object
	// list.
	// - m_idx == m_objInfo.size means return the first entry in the directories
	// list.
	// - m_idx in (m_directories.size, -1] means return an entry from the
	// directories list.
	// - m_idx == -m_directories.size means that all the path elements have been
	// consumed.
	auto idx = m_idx;
	if (m_objInfo.empty() && m_directories.empty()) {
		*buff = '\0';
		return XrdOssOK;
	} else if (idx >= 0 && idx < static_cast<ssize_t>(m_objInfo.size())) {
		// Return a file entry
		m_idx++;
		std::string full_name = m_objInfo[idx].m_key;
		auto lastSlashIdx = full_name.rfind("/");
		if (lastSlashIdx != std::string::npos) {
			full_name.erase(0, lastSlashIdx + 1);
		}

		strncpy(buff, full_name.c_str(), blen);
		if (buff[blen - 1] != '\0') {
			buff[blen - 1] = '\0';
			return -ENOMEM;
		}

		if (m_stat_buf) {
			m_stat_buf->st_mode = 0x0600 | S_IFREG;
			m_stat_buf->st_nlink = 1;
			m_stat_buf->st_size = m_objInfo[idx].m_size;
			time_t timestamp = parseTimestamp(m_objInfo[idx].m_last_modified);
			if (timestamp != 0) {
				m_stat_buf->st_mtime = timestamp;
				m_stat_buf->st_atime = timestamp;
				m_stat_buf->st_ctime = timestamp;
			}
		}
	} else if (idx < 0 && -idx == static_cast<ssize_t>(m_directories.size())) {
		// All items have been consumed
		*buff = '\0';
		return XrdOssOK;
	} else if (idx == static_cast<ssize_t>(m_objInfo.size()) ||
			   -idx < static_cast<ssize_t>(m_directories.size())) {
		// Handle directory entries
		if (m_directories.empty()) {
			*buff = '\0';
			return XrdOssOK;
		}
		if (idx == static_cast<ssize_t>(m_objInfo.size())) {
			m_idx = -1;
			idx = 0;
		} else {
			idx = -m_idx;
			m_idx--;
		}
		std::string full_name = m_directories[idx].m_key;
		if (!full_name.empty() && full_name.back() == '/') {
			full_name.pop_back();
		}

		strncpy(buff, full_name.c_str(), blen);
		if (buff[blen - 1] != '\0') {
			buff[blen - 1] = '\0';
			return -ENOMEM;
		}

		if (m_stat_buf) {
			m_stat_buf->st_mode = 0x0700 | S_IFDIR;
			m_stat_buf->st_nlink = 2;
			m_stat_buf->st_size = 4096;
			time_t timestamp =
				parseTimestamp(m_directories[idx].m_last_modified);
			if (timestamp != 0) {
				m_stat_buf->st_mtime = timestamp;
				m_stat_buf->st_atime = timestamp;
				m_stat_buf->st_ctime = timestamp;
			}
		}
	} else {
		return -EBADF;
	}

	if (m_stat_buf) {
		m_stat_buf->st_uid = 1;
		m_stat_buf->st_gid = 1;
		if (m_stat_buf->st_mtime == 0) {
			m_stat_buf->st_mtime = m_stat_buf->st_ctime = m_stat_buf->st_atime =
				0;
		}
		m_stat_buf->st_dev = 0;
		m_stat_buf->st_ino = 1;
	}
	return XrdOssOK;
}

int GlobusDirectory::StatRet(struct stat *buf) {
	if (!m_opened) {
		return -EBADF;
	}

	m_stat_buf = buf;
	return XrdOssOK;
}

int GlobusDirectory::Close(long long *retsz) {
	if (!m_opened) {
		return -EBADF;
	}
	Reset();
	return XrdOssOK;
}
