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

#include "S3Directory.hh"
#include "S3Commands.hh"
#include "logging.hh"
#include "stl_string_utils.hh"

#include <XrdSys/XrdSysError.hh>

#include <tinyxml2.h>

void S3Directory::Reset() {
	m_opened = false;
	m_ct = "";
	m_idx = 0;
	m_objInfo.clear();
	m_commonPrefixes.clear();
	m_stat_buf = nullptr;
	m_ai = S3AccessInfo();
	m_object = "";
}

int S3Directory::ListS3Dir(const std::string &ct) {
	AmazonS3List listCommand(m_ai, m_object, m_log);
	auto res = listCommand.SendRequest(ct);
	if (!res) {
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
	std::string errMsg;
	m_idx = 0;
	res = listCommand.Results(m_objInfo, m_commonPrefixes, m_ct, errMsg);
	if (!res) {
		m_log.Log(XrdHTTPServer::Warning, "Opendir",
				  "Failed to parse S3 results:", errMsg.c_str());
		return -EIO;
	}
	m_opened = true;
	return 0;
}

int S3Directory::Opendir(const char *path, XrdOucEnv &env) {
	if (m_opened) {
		return -EBADF;
	}
	Reset();

	std::string exposedPath, object;
	int rv = m_fs.parsePath(path, exposedPath, object);
	if (rv != 0) {
		return rv;
	}

	auto ai = m_fs.getS3AccessInfo(exposedPath);
	if (!ai) {
		return -ENOENT;
	}
	m_ai = *ai;
	m_object = object;

	return ListS3Dir("");
}

int S3Directory::Readdir(char *buff, int blen) {
	if (!m_opened) {
		return -EBADF;
	}

	memset(m_stat_buf, '\0', sizeof(struct stat));

	// m_idx encodes the location inside the current directory.
	// - m_idx in [0, m_objInfo.size) means return a "file" from the object
	// list.
	// - m_idx == m_objectInfo.size means return the first entry in the
	// directory/common prefix list.
	// - m_idx in (m_commonPrefixes.size, -1] means return an entry from the
	// common prefix list.
	// - m_idx == -m_commonPrefixes.size means that all the path elements have
	// been consumed.
	//
	// If all the paths entry have been consumed, then if the continuation token
	// is set, list more objects in the bucket.  If it's unset, then we've
	// iterated through all the bucket contents.
	auto idx = m_idx;
	if (m_objInfo.empty() && m_commonPrefixes.empty()) {
		*buff = '\0';
		return XrdOssOK;
	} else if (idx >= 0 && idx < static_cast<ssize_t>(m_objInfo.size())) {
		m_idx++;
		std::string full_name = m_objInfo[idx].m_key;
		full_name.erase(0, full_name.rfind("/"));
		trimslashes(full_name);
		strncpy(buff, full_name.c_str(), blen);
		if (buff[blen - 1] != '\0') {
			buff[blen - 1] = '\0';
			return -ENOMEM;
		}
		if (m_stat_buf) {
			m_stat_buf->st_mode = 0x0600 | S_IFREG;
			m_stat_buf->st_nlink = 1;
			m_stat_buf->st_size = m_objInfo[idx].m_size;
		}
	} else if (idx < 0 &&
			   -idx == static_cast<ssize_t>(m_commonPrefixes.size())) {
		if (!m_ct.empty()) {
			// Get the next set of results from S3.
			m_idx = 0;
			m_objInfo.clear();
			m_commonPrefixes.clear();
			memset(&m_stat_buf, '\0', sizeof(struct stat));
			auto rv = ListS3Dir(m_ct);
			if (rv != 0) {
				m_opened = false;
				return rv;
			}
			// Recurse to parse the fresh results.
			return Readdir(buff, blen);
		}
		*buff = '\0';
		return XrdOssOK;
	} else if (idx == static_cast<ssize_t>(m_objInfo.size()) ||
			   -idx < static_cast<ssize_t>(m_commonPrefixes.size())) {
		if (m_commonPrefixes.empty()) {
			if (!m_ct.empty()) {
				// Get the next set of results from S3.
				m_idx = 0;
				m_objInfo.clear();
				m_commonPrefixes.clear();
				memset(&m_stat_buf, '\0', sizeof(struct stat));
				auto rv = ListS3Dir(m_ct);
				if (rv != 0) {
					m_opened = false;
					return rv;
				}
				// Recurse to parse the fresh results.
				return Readdir(buff, blen);
			}
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
		std::string full_name = m_commonPrefixes[idx];
		trimslashes(full_name);
		full_name.erase(0, full_name.rfind("/"));
		trimslashes(full_name);
		strncpy(buff, full_name.c_str(), blen);
		if (buff[blen - 1] != '\0') {
			buff[blen - 1] = '\0';
			return -ENOMEM;
		}
		if (m_stat_buf) {
			m_stat_buf->st_mode = 0x0700 | S_IFDIR;
			m_stat_buf->st_nlink = 0;
			m_stat_buf->st_size = 4096;
		}
	} else {
		return -EBADF;
	}

	if (m_stat_buf) {
		m_stat_buf->st_uid = 1;
		m_stat_buf->st_gid = 1;
		m_stat_buf->st_mtime = m_stat_buf->st_ctime = m_stat_buf->st_atime = 0;
		m_stat_buf->st_dev = 0;
		m_stat_buf->st_ino = 1; // If both st_dev and st_ino are 0, then XRootD
								// interprets that as an unavailable file.
	}
	return XrdOssOK;
}

int S3Directory::StatRet(struct stat *buf) {
	if (!m_opened) {
		return -EBADF;
	}

	m_stat_buf = buf;
	return XrdOssOK;
}

int S3Directory::Close(long long *retsz) {
	if (!m_opened) {
		return -EBADF;
	}
	Reset();
	return XrdOssOK;
}
