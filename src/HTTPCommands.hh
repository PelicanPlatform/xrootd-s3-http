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

#pragma once

#include "TokenFile.hh"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <curl/curl.h>

class XrdSysError;
class HandlerQueue;
class CurlWorker;

class HTTPRequest {
	friend class CurlWorker;

  public:
	HTTPRequest(const std::string &hostUrl, XrdSysError &log,
				const TokenFile *token)
		: hostUrl(hostUrl), m_header_list(nullptr, &curl_slist_free_all),
		  m_log(log), m_token(token) {
		// Parse the URL and populate
		// What to do if the function returns false?
		// TODO: Figure out best way to deal with this
		if (!parseProtocol(hostUrl, m_protocol)) {
			errorCode = "E_INVALID_HOST_URL";
			errorMessage = "Failed to parse protocol from host/service URL.";
		}
	}
	virtual ~HTTPRequest();

	virtual const std::string *getAccessKey() const { return nullptr; }
	virtual const std::string *getSecretKey() const { return nullptr; }

	virtual bool parseProtocol(const std::string &url, std::string &protocol);

	virtual bool SendHTTPRequest(const std::string &payload);

	unsigned long getResponseCode() const { return responseCode; }
	const std::string &getErrorCode() const { return errorCode; }
	const std::string &getErrorMessage() const { return errorMessage; }
	const std::string &getResultString() const { return m_result; }

	// Currently only used in PUTS, but potentially useful elsewhere
	struct Payload {
		const std::string *data;
		size_t sentSoFar;
	};

	// Initialize libraries for HTTP.
	//
	// Should be called at least once per application from a non-threaded
	// context.
	static void Init(XrdSysError &);

  protected:
	bool sendPreparedRequest(const std::string &uri,
							 const std::string &payload);

	const std::string &getProtocol() { return m_protocol; }

	typedef std::map<std::string, std::string> AttributeValueMap;
	AttributeValueMap query_parameters;
	AttributeValueMap headers;

	std::string hostUrl;

	bool requiresSignature{false};
	struct timespec signatureTime;

	std::string errorMessage;
	std::string errorCode;

	std::string m_result;
	unsigned long responseCode{0};
	unsigned long expectedResponseCode = 200;
	bool includeResponseHeader{false};

	std::string httpVerb{"POST"};
	std::unique_ptr<HTTPRequest::Payload> m_callback_payload;

	std::unique_ptr<struct curl_slist, void (*)(struct curl_slist *)>
		m_header_list; // Headers associated with the request

	XrdSysError &m_log;

  private:
	enum class CurlResult { Ok, Fail, Retry };

	void Notify(); // Notify the main request thread the request has been
				   // processed by a worker
	virtual bool SetupHandle(
		CURL *curl); // Configure the curl handle to be used by a given request.
	CurlResult ProcessCurlResult(
		CURL *curl,
		CURLcode rv); // Process a curl command that ran to completion.
	bool
	Fail(const std::string &ecode,
		 const std::string &emsg); // Record a failure occurring for the request
								   // (curl request did not complete)
	bool ReleaseHandle(
		CURL *curl); // Cleanup any resources associated with the curl handle

	const TokenFile *m_token{nullptr};

	// The following members manage the work queue and workers.
	static bool
		m_workers_initialized; // The global state of the worker initialization.
	static std::shared_ptr<HandlerQueue>
		m_queue; // Global queue for all HTTP requests to be processed.
	static std::vector<CurlWorker *>
		m_workers; // Set of all the curl worker threads.

	// The following variables manage the state of the request.
	std::mutex
		m_mtx; // Mutex guarding the results from the curl worker's callback
	std::condition_variable m_cv; // Condition variable to notify the curl
								  // worker completed the callback
	bool m_result_ready{false};	  // Flag indicating the results data is ready.
	std::string m_protocol;
	std::string m_uri; // URL to request from libcurl
	std::string m_payload;
	char m_errorBuffer[CURL_ERROR_SIZE]; // Static error buffer for libcurl
	unsigned m_retry_count{0};
};

class HTTPUpload : public HTTPRequest {
  public:
	HTTPUpload(const std::string &h, const std::string &o, XrdSysError &log,
			   const TokenFile *token)
		: HTTPRequest(h, log, token), object(o) {
		hostUrl = hostUrl + "/" + object;
	}

	virtual ~HTTPUpload();

	virtual bool SendRequest(const std::string &payload, off_t offset,
							 size_t size);

  protected:
	std::string object;
	std::string path;
};

class HTTPDownload : public HTTPRequest {
  public:
	HTTPDownload(const std::string &h, const std::string &o, XrdSysError &log,
				 const TokenFile *token)
		: HTTPRequest(h, log, token), object(o) {
		hostUrl = hostUrl + "/" + object;
	}

	virtual ~HTTPDownload();

	virtual bool SendRequest(off_t offset, size_t size);

  protected:
	std::string object;
};

class HTTPHead : public HTTPRequest {
  public:
	HTTPHead(const std::string &h, const std::string &o, XrdSysError &log,
			 const TokenFile *token)
		: HTTPRequest(h, log, token), object(o) {
		hostUrl = hostUrl + "/" + object;
	}

	virtual ~HTTPHead();

	virtual bool SendRequest();

  protected:
	std::string object;
};
