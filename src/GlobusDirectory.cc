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
#include "HTTPCommands.hh"
#include "logging.hh"
#include "stl_string_utils.hh"

#include <XrdSys/XrdSysError.hh>

#include <sstream>

void GlobusDirectory::Reset() {
	m_opened = false;
	m_continuation_token = "";
	m_idx = 0;
	m_objInfo.clear();
	m_commonPrefixes.clear();
	m_stat_buf = nullptr;
	m_object = "";
}

int GlobusDirectory::ListGlobusDir(const std::string &continuation_token) {
	// Construct the Globus Transfer API endpoint for listing
	std::string endpoint = m_fs->getGlobusEndpoint();
	std::string collection_id = m_fs->getGlobusCollectionId();
	
	// Build the URL for the Globus Transfer API list operation
	std::string url = endpoint + "/v0.10/operation/endpoint/" + collection_id + "/ls";
	
	// Add path parameter if we have a specific path to list
	if (!m_object.empty()) {
		url += "?path=" + m_object;
	}
	
	// Add continuation token if provided
	if (!continuation_token.empty()) {
		url += (m_object.empty() ? "?" : "&") + std::string("marker=") + continuation_token;
	}

	// Use HTTPHead to make the request (we'll use HTTPDownload for the actual listing)
	HTTPDownload listCommand(url, "", m_log, m_fs->getGlobusToken());
	
	if (!listCommand.SendRequest(0, 0)) { // Request all data
		switch (listCommand.getResponseCode()) {
		case 404:
			return -ENOENT;
		case 500:
			return -EIO;
		case 403:
			return -EPERM;
		default:
			return -EIO;
		}
	}

	// Parse the JSON response from Globus
	std::string response = listCommand.getResultString();
	
	// TODO: Parse JSON response and populate m_objInfo and m_commonPrefixes
	// This is a placeholder implementation - you'll need to implement the actual JSON parsing
	// based on the Globus Transfer API response format
	
	// Example JSON structure from Globus Transfer API:
	// {
	//   "DATA": [
	//     {
	//       "DATA_TYPE": "file",
	//       "name": "example.txt",
	//       "size": 1024,
	//       "last_modified": "2024-01-01T00:00:00Z"
	//     }
	//   ],
	//   "next_marker": "next_token_here"
	// }
	
	// For now, set up basic structure
	m_idx = 0;
	m_opened = true;
	
	// TODO: Implement JSON parsing here
	// Json::Value root;
	// Json::Reader reader;
	// if (reader.parse(response, root)) {
	//     if (root.isMember("DATA")) {
	//         const Json::Value& data = root["DATA"];
	//         for (const Json::Value& item : data) {
	//             GlobusObjectInfo obj;
	//             obj.m_key = item["name"].asString();
	//             obj.m_size = item["size"].asUInt64();
	//             obj.m_last_modified = item["last_modified"].asString();
	//             m_objInfo.push_back(obj);
	//         }
	//     }
	//     if (root.isMember("next_marker")) {
	//         m_continuation_token = root["next_marker"].asString();
	//     }
	// }
	
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

	// Parse the path to extract the object path within the Globus collection
	// This is similar to how HTTPFileSystem parses paths
	std::string storagePrefix = m_fs->getStoragePrefix();
	std::string object;
	
	// Simple path parsing - you may need to adjust this based on your needs
	if (realPath.find(storagePrefix) == 0) {
		object = realPath.substr(storagePrefix.length());
	} else {
		object = realPath;
	}
	
	// Remove leading slash if present
	if (!object.empty() && object[0] == '/') {
		object = object.substr(1);
	}
	
	m_object = object;

	return ListGlobusDir("");
}

int GlobusDirectory::Readdir(char *buff, int blen) {
	if (!m_opened) {
		return -EBADF;
	}

	if (m_stat_buf) {
		memset(m_stat_buf, '\0', sizeof(struct stat));
	}

	// Check if we need to fetch more results
	if (m_idx >= static_cast<ssize_t>(m_objInfo.size())) {
		if (!m_continuation_token.empty()) {
			// Get the next set of results from Globus
			m_idx = 0;
			m_objInfo.clear();
			m_commonPrefixes.clear();
			if (m_stat_buf) {
				memset(m_stat_buf, '\0', sizeof(struct stat));
			}
			auto rv = ListGlobusDir(m_continuation_token);
			if (rv != 0) {
				m_opened = false;
				return rv;
			}
			// Recurse to parse the fresh results
			return Readdir(buff, blen);
		}
		*buff = '\0';
		return XrdOssOK;
	}

	// Return the next object name
	std::string full_name = m_objInfo[m_idx].m_key;
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
		m_stat_buf->st_size = m_objInfo[m_idx].m_size;
		m_stat_buf->st_uid = 1;
		m_stat_buf->st_gid = 1;
		m_stat_buf->st_mtime = m_stat_buf->st_ctime = m_stat_buf->st_atime = 0;
		m_stat_buf->st_dev = 0;
		m_stat_buf->st_ino = 1;
	}
	
	m_idx++;
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