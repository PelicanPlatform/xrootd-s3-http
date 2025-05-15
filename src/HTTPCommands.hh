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

#pragma once

#include "TokenFile.hh"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
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

	// State of the payload upload for the curl callbacks
	struct Payload {
		std::string_view data;
		off_t sentSoFar{0};
		bool final{true};
		HTTPRequest &m_parent;

		void NotifyPaused(); // Notify the parent request the curl handle has
							 // been paused
	};

	// Initialize libraries for HTTP.
	//
	// Should be called at least once per application from a non-threaded
	// context.
	static void Init(XrdSysError &);

	// Perform maintenance of the request.
	void Tick(std::chrono::steady_clock::time_point);

	// Sets the duration after which an in-progress operation may be considered
	// stalled and hence timeout.
	static void SetStallTimeout(std::chrono::steady_clock::duration timeout) {
		m_timeout_duration = timeout;
	}

	// Return the stall timeout duration currently in use.
	static std::chrono::steady_clock::duration GetStallTimeout() {
		return m_timeout_duration;
	}

	// Handle HTTP request errors and convert them to appropriate POSIX error
	// codes. This function can be used anywhere HTTP requests are made to
	// provide consistent error handling.
	//
	// - request: The HTTPRequest object that was used for the request
	// - log: The logger instance for error reporting
	// - operation: A string describing the operation being performed (for
	// logging)
	// - context: Additional context information (for logging)
	//
	// Returns: A POSIX error code (-ENOENT, -EIO, -EPERM, etc.) or 0 if no
	// error
	static int HandleHTTPError(const HTTPRequest &request, XrdSysError &log,
							   const char *operation,
							   const char *context = nullptr);

  protected:
	// Send the request to the HTTP server.
	// Blocks until the request has completed.
	// If `final` is set to `false`, the HTTPRequest object will start streaming
	// a request and assume that `sendPreparedRequest` will be repeated until
	// all data is provided (the sum total of the chunks given is the
	// payload_size). If payload_size is 0 and final is false, this indicates
	// the complete size of the PUT is unknown and chunked encoding will be
	// used.
	//
	// - url: URL, including query parameters, to use.
	// - payload: The payload contents when uploading.
	// - payload_size: Size of the entire payload (not just the current chunk).
	// - final: True if this is the last or only payload for the request.  False
	// otherwise.
	bool sendPreparedRequest(const std::string &url,
							 const std::string_view payload, off_t payload_size,
							 bool final);

	// Send the request to the HTTP server.
	// Returns immediately, not waiting for the result.
	//
	// If `final` is set to `false`, the HTTPRequest object will start streaming
	// a request and assume that `sendPreparedRequest` will be repeated until
	// all data is provided (the sum total of the chunks given is the
	// payload_size). If payload_size is 0 and final is false, this indicates
	// the complete size of the PUT is unknown and chunked encoding will be
	// used.
	//
	// - url: URL, including query parameters, to use.
	// - payload: The payload contents when uploading.
	// - payload_size: Size of the entire payload (not just the current chunk).
	// - final: True if this is the last or only payload for the request.  False
	// otherwise.
	bool sendPreparedRequestNonblocking(const std::string &uri,
										const std::string_view payload,
										off_t payload_size, bool final);

	// Called by the curl handler thread that the request has been finished.
	virtual void Notify();

	// Returns the standalone buffer if a sub-classe's externally-managed one
	// is supposed to be used.
	//
	// If the std::string_view is empty, then it's assumed the HTTPRequest
	// itself owns the result buffer and should create one.  Note that,
	// on errors, the HTTPRequest result buffer is still used.
	virtual std::string_view *requestResult() { return nullptr; }

	const std::string &getProtocol() { return m_protocol; }

	// Returns true if the command is a streaming/partial request.
	// A streaming request is one that requires multiple calls to
	// `sendPreparedRequest` to complete.
	bool isStreamingRequest() const { return m_is_streaming; }

	// Record the unpause queue associated with this request.
	//
	// Future continuations of this request will be sent directly to this queue.
	void SetUnpauseQueue(std::shared_ptr<HandlerQueue> queue) {
		m_unpause_queue = queue;
	}

	// Return whether or not the request has timed out since the last
	// call to send more data.
	bool Timeout() const { return m_timeout; }

	// Function that can be overridden by test cases, allowing modification
	// of the server response
	virtual void modifyResponse(std::string &) {}

	typedef std::map<std::string, std::string> AttributeValueMap;
	AttributeValueMap query_parameters;
	AttributeValueMap headers;

	std::string hostUrl;

	bool requiresSignature{false};
	struct timespec signatureTime;

	std::string errorMessage;
	std::string errorCode;

	// The contents of the result from the HTTP server.
	// If this is a GET and we got the expectedResponseCode, then
	// the results are populated in the m_result_buffer instead.
	std::string m_result;
	unsigned long responseCode{0};
	std::unordered_set<unsigned long> expectedResponseCode{200};
	bool includeResponseHeader{false};

	std::string httpVerb{"POST"};
	std::unique_ptr<HTTPRequest::Payload> m_callback_payload;

	std::unique_ptr<struct curl_slist, void (*)(struct curl_slist *)>
		m_header_list; // Headers associated with the request

	XrdSysError &m_log;

  private:
	virtual bool SetupHandle(
		CURL *curl); // Configure the curl handle to be used by a given request.

	virtual bool
	ContinueHandle(); // Continue the request processing after a pause.

	void ProcessCurlResult(
		CURL *curl,
		CURLcode rv); // Process a curl command that ran to completion.

	bool
	Fail(const std::string &ecode,
		 const std::string &emsg); // Record a failure occurring for the request
								   // (curl request did not complete)
	bool ReleaseHandle(
		CURL *curl); // Cleanup any resources associated with the curl handle
	CURL *getHandle() const { return m_curl_handle; }

	// Callback for libcurl when the library is ready to read more data from our
	// buffer.
	static size_t ReadCallback(char *buffer, size_t size, size_t n, void *v);

	// Handle the callback from libcurl
	static size_t handleResults(const void *ptr, size_t size, size_t nmemb,
								void *me_ptr);

	// Transfer information callback from libcurl
	static int XferInfoCallback(void *clientp, curl_off_t dltotal,
								curl_off_t dlnow, curl_off_t ultotal,
								curl_off_t ulnow);

	const TokenFile *m_token{nullptr};

	// The following members manage the work queue and workers.
	static bool
		m_workers_initialized; // The global state of the worker initialization.
	static std::shared_ptr<HandlerQueue>
		m_queue; // Global queue for all HTTP requests to be processed.
	std::shared_ptr<HandlerQueue> m_unpause_queue{
		nullptr}; // Queue to notify the request can be resumed.
	static std::vector<CurlWorker *>
		m_workers; // Set of all the curl worker threads.

	// The following variables manage the state of the request.
	std::mutex
		m_mtx; // Mutex guarding the results from the curl worker's callback

	// Condition variable to notify the curl worker completed the callback.
	std::condition_variable m_cv;

	bool m_final{false}; // Flag indicating this is the last sendPreparedRequest
						 // call of the overall HTTPRequest
	bool m_is_streaming{
		false}; // Flag indicating this command is a streaming request.
	bool m_timeout{false};		// Flag indicating the request has timed out.
	bool m_result_ready{false}; // Flag indicating the results data is ready.
	bool m_result_buffer_initialized{
		false}; // Flag indicating whether the result buffer view has been
				// initialized.
	off_t m_payload_size{0}; // Size of the entire upload payload; 0 if unknown.
	std::string m_protocol;
	std::string m_uri; // URL to request from libcurl
	std::string_view m_payload;

	// Total number of bytes received from the server
	off_t m_bytes_recv{0};
	// Total number of bytes sent to server
	off_t m_bytes_sent{0};
	// Time of last data movement (upload or download).  Used to detect transfer
	// stalls
	std::chrono::steady_clock::time_point m_last_movement;
	// Transfer stall timeout
	static constexpr std::chrono::steady_clock::duration m_transfer_stall{
		std::chrono::seconds(9)};

	// The contents of a successful GET request.
	std::string_view m_result_buffer;
	CURL *m_curl_handle{nullptr}; // The curl handle for the ongoing request
	char m_errorBuffer[CURL_ERROR_SIZE]; // Static error buffer for libcurl

	// Time when the last request was sent on this object; used to determine
	// whether the operation has timed out.
	std::chrono::steady_clock::time_point m_last_request{
		std::chrono::steady_clock::now()};

	// Duration after which a partially-completed request will timeout if
	// no progress has been made.
	static std::chrono::steady_clock::duration m_timeout_duration;
};

class HTTPUpload final : public HTTPRequest {
  public:
	HTTPUpload(const std::string &h, const std::string &o, XrdSysError &log,
			   const TokenFile *token)
		: HTTPRequest(h, log, token), object(o) {
		hostUrl = hostUrl + "/" + object;
	}

	virtual ~HTTPUpload();

	virtual bool SendRequest(const std::string &payload);

	// Start a streaming request.
	//
	// - payload: The payload contents when uploading.
	// - object_size: Size of the entire upload payload.
	bool StartStreamingRequest(const std::string_view payload,
							   off_t object_size);

	// Continue a streaming request.
	//
	// - payload: The payload contents when uploading.
	// - object_size: Size of the entire upload payload.
	// - final: True if this is the last or only payload for the request.  False
	// otherwise.
	bool ContinueStreamingRequest(const std::string_view payload,
								  off_t object_size, bool final);

  protected:
	std::string object;
	std::string path;
};

class HTTPDownload final : public HTTPRequest {
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

class HTTPList final : public HTTPRequest {
  public:
	HTTPList(const std::string &h, const std::string &o, XrdSysError &log,
			 const TokenFile *token)
		: HTTPRequest(h, log, token), object(o) {
		hostUrl = hostUrl + "/" + object;
	}

	virtual ~HTTPList();

	virtual bool SendRequest();

  protected:
	std::string object;
};

class HTTPHead final : public HTTPRequest {
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

class HTTPDelete final : public HTTPRequest {
  public:
	HTTPDelete(const std::string &h, const std::string &o, XrdSysError &log,
			   const TokenFile *token)
		: HTTPRequest(h, log, token), object(o) {
		hostUrl = hostUrl + "/" + object;
	}

	virtual ~HTTPDelete();

	virtual bool SendRequest();

  protected:
	std::string object;
};
