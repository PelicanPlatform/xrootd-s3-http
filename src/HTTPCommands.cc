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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include <XrdSys/XrdSysError.hh>
#include <curl/curl.h>
#include <openssl/hmac.h>

#include "CurlUtil.hh"
#include "CurlWorker.hh"
#include "HTTPCommands.hh"
#include "logging.hh"
#include "shortfile.hh"
#include "stl_string_utils.hh"

using namespace XrdHTTPServer;

std::shared_ptr<HandlerQueue> HTTPRequest::m_queue =
	std::make_unique<HandlerQueue>();
bool HTTPRequest::m_workers_initialized = false;
std::vector<CurlWorker *> HTTPRequest::m_workers;

namespace {

//
// "This function gets called by libcurl as soon as there is data received
//  that needs to be saved. The size of the data pointed to by ptr is size
//  multiplied with nmemb, it will not be zero terminated. Return the number
//  of bytes actually taken care of. If that amount differs from the amount
//  passed to your function, it'll signal an error to the library. This will
//  abort the transfer and return CURLE_WRITE_ERROR."
//
// We also make extensive use of this function in the XML parsing code,
// for pretty much exactly the same reason.
//
size_t appendToString(const void *ptr, size_t size, size_t nmemb, void *str) {
	if (size == 0 || nmemb == 0) {
		return 0;
	}

	std::string source((const char *)ptr, size * nmemb);
	std::string *ssptr = (std::string *)str;
	ssptr->append(source);

	return (size * nmemb);
}

} // namespace

HTTPRequest::~HTTPRequest() {}

#define SET_CURL_SECURITY_OPTION(A, B, C)                                      \
	{                                                                          \
		CURLcode rv##B = curl_easy_setopt(A, B, C);                            \
		if (rv##B != CURLE_OK) {                                               \
			errorCode = "E_CURL_LIB";                                          \
			errorMessage = "curl_easy_setopt( " #B " ) failed.";               \
			return false;                                                      \
		}                                                                      \
	}

bool HTTPRequest::parseProtocol(const std::string &url, std::string &protocol) {

	auto i = url.find("://");
	if (i == std::string::npos) {
		return false;
	}
	protocol = substring(url, 0, i);

	return true;
}

bool HTTPRequest::SendHTTPRequest(const std::string &payload) {
	if ((m_protocol != "http") && (m_protocol != "https")) {
		errorCode = "E_INVALID_SERVICE_URL";
		errorMessage = "Service URL not of a known protocol (http[s]).";
		m_log.Log(LogMask::Warning, "HTTPRequest::SendHTTPRequest",
				  "Host URL '", hostUrl.c_str(),
				  "' not of a known protocol (http[s]).");
		return false;
	}

	headers["Content-Type"] = "binary/octet-stream";
	std::string contentLength;
	formatstr(contentLength, "%zu", payload.size());
	headers["Content-Length"] = contentLength;
	// Another undocumented CURL feature: transfer-encoding is "chunked"
	// by default for "PUT", which we really don't want.
	headers["Transfer-Encoding"] = "";

	return sendPreparedRequest(hostUrl, payload);
}

static void dump(const char *text, FILE *stream, unsigned char *ptr,
				 size_t size) {
	size_t i;
	size_t c;
	unsigned int width = 0x10;

	fprintf(stream, "%s, %10.10ld bytes (0x%8.8lx)\n", text, (long)size,
			(long)size);

	for (i = 0; i < size; i += width) {
		fprintf(stream, "%4.4lx: ", (long)i);

		/* show hex to the left */
		for (c = 0; c < width; c++) {
			if (i + c < size)
				fprintf(stream, "%02x ", ptr[i + c]);
			else
				fputs("   ", stream);
		}

		/* show data on the right */
		for (c = 0; (c < width) && (i + c < size); c++) {
			char x =
				(ptr[i + c] >= 0x20 && ptr[i + c] < 0x80) ? ptr[i + c] : '.';
			fputc(x, stream);
		}

		fputc('\n', stream); /* newline */
	}
}

static void dump_plain(const char *text, FILE *stream, unsigned char *ptr,
					   size_t size) {
	fprintf(stream, "%s, %10.10ld bytes (0x%8.8lx)\n", text, (long)size,
			(long)size);
	fwrite(ptr, 1, size, stream);
	fputs("\n", stream);
}

int debugCallback(CURL *handle, curl_infotype ci, char *data, size_t size,
				  void *clientp) {
	const char *text;
	(void)handle; /* prevent compiler warning */
	(void)clientp;

	switch (ci) {
	case CURLINFO_TEXT:
		fputs("== Info: ", stderr);
		fwrite(data, size, 1, stderr);
	default: /* in case a new one is introduced to shock us */
		return 0;

	case CURLINFO_HEADER_OUT:
		text = "=> Send header";
		dump_plain(text, stderr, (unsigned char *)data, size);
		break;
	}
	return 0;
}

int debugAndDumpCallback(CURL *handle, curl_infotype ci, char *data,
						 size_t size, void *clientp) {
	const char *text;
	(void)handle; /* prevent compiler warning */
	(void)clientp;

	switch (ci) {
	case CURLINFO_TEXT:
		fputs("== Info: ", stderr);
		fwrite(data, size, 1, stderr);
	default: /* in case a new one is introduced to shock us */
		return 0;

	case CURLINFO_HEADER_OUT:
		text = "=> Send header";
		dump_plain(text, stderr, (unsigned char *)data, size);
		break;
	case CURLINFO_DATA_OUT:
		text = "=> Send data";
		break;
	case CURLINFO_SSL_DATA_OUT:
		text = "=> Send SSL data";
		break;
	case CURLINFO_HEADER_IN:
		text = "<= Recv header";
		break;
	case CURLINFO_DATA_IN:
		text = "<= Recv data";
		break;
	case CURLINFO_SSL_DATA_IN:
		text = "<= Recv SSL data";
		break;
	}
	dump(text, stderr, (unsigned char *)data, size);
	return 0;
}

// A callback function that gets passed to curl_easy_setopt for reading data
// from the payload
size_t read_callback(char *buffer, size_t size, size_t n, void *v) {
	// The callback gets the void pointer that we set with CURLOPT_READDATA. In
	// this case, it's a pointer to an HTTPRequest::Payload struct that contains
	// the data to be sent, along with the offset of the data that has already
	// been sent.
	HTTPRequest::Payload *payload = (HTTPRequest::Payload *)v;

	if (payload->sentSoFar == payload->data->size()) {
		payload->sentSoFar = 0;
		return 0;
	}

	size_t request = size * n;
	if (request > payload->data->size()) {
		request = payload->data->size();
	}

	if (payload->sentSoFar + request > payload->data->size()) {
		request = payload->data->size() - payload->sentSoFar;
	}

	memcpy(buffer, payload->data->data() + payload->sentSoFar, request);
	payload->sentSoFar += request;

	return request;
}

bool HTTPRequest::sendPreparedRequest(const std::string &uri,
									  const std::string &payload) {
	m_uri = uri;
	m_payload = payload;

	m_queue->Produce(this);
	std::unique_lock<std::mutex> lk(m_mtx);
	m_cv.wait(lk, [&] { return m_result_ready; });

	return errorCode.empty();
}

bool HTTPRequest::ReleaseHandle(CURL *curl) {
	if (curl == nullptr)
		return false;
	// Note: Any option that's conditionally set in `HTTPRequest::SetupHandle`
	// must be restored to the original state here.
	//
	// Only changing back the things we explicitly set is a conscious decision
	// here versus using `curl_easy_reset`; we are trying to avoid whacking
	// all the configuration of the handle.
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, nullptr);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, nullptr);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);
	curl_easy_setopt(curl, CURLOPT_OPENSOCKETFUNCTION, nullptr);
	curl_easy_setopt(curl, CURLOPT_OPENSOCKETDATA, nullptr);
	curl_easy_setopt(curl, CURLOPT_SOCKOPTFUNCTION, nullptr);
	curl_easy_setopt(curl, CURLOPT_SOCKOPTDATA, nullptr);
	curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, nullptr);
	curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
	curl_easy_setopt(curl, CURLOPT_NOBODY, 0);
	curl_easy_setopt(curl, CURLOPT_POST, 0);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 0);
	curl_easy_setopt(curl, CURLOPT_HEADER, 0);
	curl_easy_setopt(curl, CURLOPT_SSLCERT, nullptr);
	curl_easy_setopt(curl, CURLOPT_SSLKEY, nullptr);

	return true;
}

bool HTTPRequest::SetupHandle(CURL *curl) {
	m_log.Log(XrdHTTPServer::Debug, "SetupHandle", "Sending HTTP request",
			  m_uri.c_str());

	if (curl == nullptr) {
		errorCode = "E_CURL_LIB";
		errorMessage = "curl_easy_init() failed.";
		return false;
	}

	auto rv = curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, m_errorBuffer);
	if (rv != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage = "curl_easy_setopt( CURLOPT_ERRORBUFFER ) failed.";
		return false;
	}

	rv = curl_easy_setopt(curl, CURLOPT_URL, m_uri.c_str());
	if (rv != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage = "curl_easy_setopt( CURLOPT_URL ) failed.";
		return false;
	}

	if (httpVerb == "HEAD") {
		rv = curl_easy_setopt(curl, CURLOPT_NOBODY, 1);
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_HEAD ) failed.";
			return false;
		}
	}

	if (httpVerb == "POST") {
		rv = curl_easy_setopt(curl, CURLOPT_POST, 1);
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_POST ) failed.";
			return false;
		}

		rv = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, m_payload.c_str());
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage =
				"curl_easy_setopt( CURLOPT_POSTFIELDS ) failed.";
			return false;
		}
	}

	if (httpVerb == "PUT") {
		rv = curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_UPLOAD ) failed.";
			return false;
		}

		// Our HTTPRequest instance should have a pointer to the payload data
		// and the offset of the data Here, we tell curl_easy_setopt to use the
		// read_callback function to read the data from the payload
		m_callback_payload = std::unique_ptr<HTTPRequest::Payload>(
			new HTTPRequest::Payload{&m_payload, 0});
		rv = curl_easy_setopt(curl, CURLOPT_READDATA, m_callback_payload.get());
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_READDATA ) failed.";
			return false;
		}

		rv = curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage =
				"curl_easy_setopt( CURLOPT_READFUNCTION ) failed.";
			return false;
		}
	}

	rv = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
	if (rv != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage = "curl_easy_setopt( CURLOPT_NOPROGRESS ) failed.";
		return false;
	}

	if (includeResponseHeader) {
		rv = curl_easy_setopt(curl, CURLOPT_HEADER, 1);
		if (rv != CURLE_OK) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_HEADER ) failed.";
			return false;
		}
	}

	rv = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &appendToString);
	if (rv != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage =
			"curl_easy_setopt( CURLOPT_WRITEFUNCTION ) failed.";
		return false;
	}

	rv = curl_easy_setopt(curl, CURLOPT_WRITEDATA, &m_result);
	if (rv != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage = "curl_easy_setopt( CURLOPT_WRITEDATA ) failed.";
		return false;
	}

	if (curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1) != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage =
			"curl_easy_setopt( CURLOPT_FOLLOWLOCATION ) failed.";
		return false;
	}

	//
	// Set security options.
	//
	SET_CURL_SECURITY_OPTION(curl, CURLOPT_SSL_VERIFYPEER, 1);
	SET_CURL_SECURITY_OPTION(curl, CURLOPT_SSL_VERIFYHOST, 2);

	std::string CAFile = "";
	std::string CAPath = "";
	auto x509_ca_dir = getenv("X509_CERT_DIR");
	if (x509_ca_dir != nullptr && x509_ca_dir[0] != '\0') {
		SET_CURL_SECURITY_OPTION(curl, CURLOPT_CAPATH, x509_ca_dir);
	}

	auto x509_ca_file = getenv("X509_CERT_FILE");
	if (x509_ca_file != nullptr) {
		SET_CURL_SECURITY_OPTION(curl, CURLOPT_CAINFO, x509_ca_file);
	}

	//
	// Configure for x.509 operation.
	//

	if (m_protocol == "x509" && requiresSignature) {
		auto accessKeyFilePtr = getAccessKey();
		auto secretKeyFilePtr = getSecretKey();
		if (accessKeyFilePtr && secretKeyFilePtr) {

			SET_CURL_SECURITY_OPTION(curl, CURLOPT_SSLKEYTYPE, "PEM");
			SET_CURL_SECURITY_OPTION(curl, CURLOPT_SSLKEY,
									 *secretKeyFilePtr->c_str());

			SET_CURL_SECURITY_OPTION(curl, CURLOPT_SSLCERTTYPE, "PEM");
			SET_CURL_SECURITY_OPTION(curl, CURLOPT_SSLCERT,
									 *accessKeyFilePtr->c_str());
		}
	}

	if (m_token) {
		const auto iter = headers.find("Authorization");
		if (iter == headers.end()) {
			std::string token;
			if (m_token->Get(token)) {
				if (!token.empty()) {
					headers["Authorization"] = "Bearer " + token;
				}
			} else {
				errorCode = "E_TOKEN";
				errorMessage = "failed to load authorization token from file";
				return false;
			}
		}
	}
	std::string headerPair;
	m_header_list.reset();
	for (auto i = headers.begin(); i != headers.end(); ++i) {
		formatstr(headerPair, "%s: %s", i->first.c_str(), i->second.c_str());
		auto tmp_headers =
			curl_slist_append(m_header_list.get(), headerPair.c_str());
		if (tmp_headers == nullptr) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_slist_append() failed.";
			return false;
		}
		m_header_list.release();
		m_header_list.reset(tmp_headers);
	}

	rv = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, m_header_list.get());
	if (rv != CURLE_OK) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage = "curl_easy_setopt( CURLOPT_HTTPHEADER ) failed.";
		return false;
	}
	if (m_log.getMsgMask() & LogMask::Debug) {
		rv = curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, debugCallback);
		rv = curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	}
	if (m_log.getMsgMask() & LogMask::Dump) {
		rv =
			curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, debugAndDumpCallback);
		rv = curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	}

	return true;
}

bool HTTPRequest::Fail(const std::string &ecode, const std::string &emsg) {
	errorCode = ecode;
	errorMessage = emsg;

	Notify();
	return true;
}

void HTTPRequest::Notify() {
	std::lock_guard<std::mutex> lk(m_mtx);
	m_result_ready = true;
	m_cv.notify_one();
}

HTTPRequest::CurlResult HTTPRequest::ProcessCurlResult(CURL *curl,
													   CURLcode rv) {

	auto cleaner = [&](void *) { Notify(); };
	auto unique = std::unique_ptr<void, decltype(cleaner)>((void *)1, cleaner);

	if (rv != 0) {
		errorCode = "E_CURL_IO";
		std::ostringstream error;
		error << "curl failed (" << rv << "): '" << curl_easy_strerror(rv)
			  << "'.";
		errorMessage = error.str();

		return CurlResult::Fail;
	}

	responseCode = 0;
	rv = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
	if (rv != CURLE_OK) {
		// So we contacted the server but it returned such gibberish that
		// CURL couldn't identify the response code.  Let's assume that's
		// bad news.  Since we're already terminally failing the request,
		// don't bother to check if this was our last chance at retrying.

		errorCode = "E_CURL_LIB";
		errorMessage = "curl_easy_getinfo() failed.";

		return CurlResult::Fail;
	}

	if (responseCode == 503 &&
		(m_result.find("<Error><Code>RequestLimitExceeded</Code>") !=
		 std::string::npos) &&
		m_retry_count == 0) {
		m_result.clear();
		m_retry_count++;
		return CurlResult::Retry;
	}

	if (responseCode != expectedResponseCode) {
		formatstr(errorCode,
				  "E_HTTP_RESPONSE_NOT_EXPECTED (response %lu != expected %lu)",
				  responseCode, expectedResponseCode);
		errorMessage = m_result;
		if (errorMessage.empty()) {
			formatstr(
				errorMessage,
				"HTTP response was %lu, not %lu, and no body was returned.",
				responseCode, expectedResponseCode);
		}
		return CurlResult::Fail;
	}

	return CurlResult::Ok;
}

// ---------------------------------------------------------------------------

HTTPUpload::~HTTPUpload() {}

bool HTTPUpload::SendRequest(const std::string &payload, off_t offset,
							 size_t size) {
	if (offset != 0 || size != 0) {
		std::string range;
		formatstr(range, "bytes=%lld-%lld", static_cast<long long int>(offset),
				  static_cast<long long int>(offset + size - 1));
		headers["Range"] = range.c_str();
	}

	httpVerb = "PUT";
	return SendHTTPRequest(payload);
}

void HTTPRequest::Init(XrdSysError &log) {
	if (!m_workers_initialized) {
		for (unsigned idx = 0; idx < CurlWorker::GetPollThreads(); idx++) {
			m_workers.push_back(new CurlWorker(m_queue, log));
			std::thread t(CurlWorker::RunStatic, m_workers.back());
			t.detach();
		}
		m_workers_initialized = true;
	}

	CURLcode rv = curl_global_init(CURL_GLOBAL_ALL);
	if (rv != 0) {
		throw std::runtime_error("libcurl failed to initialize");
	}
}

// ---------------------------------------------------------------------------

HTTPDownload::~HTTPDownload() {}

bool HTTPDownload::SendRequest(off_t offset, size_t size) {
	if (offset != 0 || size != 0) {
		std::string range;
		formatstr(range, "bytes=%lld-%lld", static_cast<long long int>(offset),
				  static_cast<long long int>(offset + size - 1));
		headers["Range"] = range.c_str();
		this->expectedResponseCode = 206;
	}

	httpVerb = "GET";
	std::string noPayloadAllowed;
	return SendHTTPRequest(noPayloadAllowed);
}

// ---------------------------------------------------------------------------

HTTPHead::~HTTPHead() {}

bool HTTPHead::SendRequest() {
	httpVerb = "HEAD";
	includeResponseHeader = true;
	std::string noPayloadAllowed;
	return SendHTTPRequest(noPayloadAllowed);
}

// ---------------------------------------------------------------------------
