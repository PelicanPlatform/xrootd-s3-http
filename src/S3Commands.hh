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

#include "HTTPCommands.hh"
#include "S3AccessInfo.hh"

#include <string>
#include <string_view>
#include <vector>

// The base class for all requests to the S3 endpoint.
// Handles common activities like signing requests and forwarding to the
// underlying HTTPRequest object.
class AmazonRequest : public HTTPRequest {
  public:
	AmazonRequest(const S3AccessInfo &ai, const std::string objectName,
				  XrdSysError &log, bool ro = true)
		: AmazonRequest(ai.getS3ServiceUrl(), ai.getS3AccessKeyFile(),
						ai.getS3SecretKeyFile(), ai.getS3BucketName(),
						objectName, ai.getS3UrlStyle(),
						ai.getS3SignatureVersion(), log, ro) {}

	AmazonRequest(const std::string &s, const std::string &akf,
				  const std::string &skf, const std::string &b,
				  const std::string &o, const std::string &style, int sv,
				  XrdSysError &log, bool ro = true)
		: HTTPRequest(s, log, nullptr), accessKeyFile(akf), secretKeyFile(skf),
		  signatureVersion(sv), bucket(b), object(o), m_style(style) {
		requiresSignature = true;
		retainObject = ro;
		// Start off by parsing the hostUrl, which we use in conjunction with
		// the bucket to fill in the host (for setting host header). For
		// example, if the incoming hostUrl (which we get from config) is
		// "https://my-url.com:443", the bucket is "my-bucket", and the object
		// is "my-object", then the host will be "my-bucket.my-url.com:443" and
		// the canonicalURI will be "/my-object".
		if (!parseURL(hostUrl, bucketPath, canonicalURI)) {
			errorCode = "E_INVALID_SERVICE_URL";
			errorMessage =
				"Failed to parse host and canonicalURI from service URL.";
		}

		if (canonicalURI.empty()) {
			canonicalURI = "/";
		}

		// Now that we have the host and canonicalURI, we can build the actual
		// url we perform the curl against. Using the previous example, we'd get
		// a new hostUrl of
		// --> "https://my-bucket.my-url.com:443/my-object" for virtual style
		// requests, and
		// --> "https://my-url.com:443/my-bucket/my-object" for path style
		// requests.
		hostUrl = getProtocol() + "://" + host + canonicalURI;

		// If we can, set the region based on the host.
		size_t secondDot = host.find(".", 2 + 1);
		if (host.find("s3.") == 0) {
			region = host.substr(3, secondDot - 2 - 1);
		}
	}
	virtual ~AmazonRequest();

	virtual const std::string *getAccessKey() const { return &accessKeyFile; }
	virtual const std::string *getSecretKey() const { return &secretKeyFile; }

	bool parseURL(const std::string &url, std::string &bucket_path,
				  std::string &path);

	virtual bool SendRequest();

	// Send a request to the S3 service.
	//
	// - payload: contents of the request itself
	// - payload_size: final size of the payload for uploads; 0 if unknown.
	// - final: True if this is the last (or only) payload of the request; false
	// otherwise
	// - blocking: True if the method should block on a response; false
	// otherwise
	virtual bool SendS3Request(const std::string_view payload,
							   off_t payload_size, bool final, bool blocking);

	static void Init(XrdSysError &log) { HTTPRequest::Init(log); }

  protected:
	// Send a request to the S3 service using the V4 signing method.
	//
	// - payload: contents of the request (for uploads or for XML-based
	// commands)
	// - payload_size: final size of the payload for uploads; 0 if unknown.
	// - sendContentSHA: Whether to add the header indicating the checksum of
	// the final payload.  Servers may verify this is what they received.
	// - final: True if this is the last (or only) payload of the request; false
	// otherwise.
	// - blocking: True if this method should block until a response; false
	// otherwise
	bool sendV4Request(const std::string_view payload, off_t payload_size,
					   bool sendContentSHA, bool final, bool blocking);

	bool retainObject;
	bool m_streamingRequest{
		false}; // Is this a streaming request?  Streaming requests will not
				// include a SHA-256 signature in the header

	std::string accessKeyFile;
	std::string secretKeyFile;

	int signatureVersion;

	std::string host;
	std::string canonicalURI;
	std::string bucketPath; // Path to use for bucket-level operations (such as
							// listings).  May be empty for DNS-style buckets
	std::string canonicalQueryString;

	std::string bucket;
	std::string object;

	std::string region;
	std::string service;

	std::string m_style;

  private:
	bool createV4Signature(const std::string_view payload,
						   std::string &authorizationHeader,
						   bool sendContentSHA = false);

	std::string canonicalizeQueryString();
};

class AmazonS3Upload final : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3Upload(const S3AccessInfo &ai, const std::string &objectName,
				   XrdSysError &log)
		: AmazonRequest(ai, objectName, log) {}

	AmazonS3Upload(const std::string &s, const std::string &akf,
				   const std::string &skf, const std::string &b,
				   const std::string &o, const std::string &style,
				   XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	virtual ~AmazonS3Upload();

	bool SendRequest(const std::string_view &payload);

  protected:
	std::string path;
};

class AmazonS3CreateMultipartUpload final : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3CreateMultipartUpload(const S3AccessInfo &ai,
								  const std::string &objectName,
								  XrdSysError &log)
		: AmazonRequest(ai, objectName, log) {}

	AmazonS3CreateMultipartUpload(const std::string &s, const std::string &akf,
								  const std::string &skf, const std::string &b,
								  const std::string &o,
								  const std::string &style, XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	bool Results(std::string &uploadId, std::string &errMsg);

	virtual ~AmazonS3CreateMultipartUpload();

	virtual bool SendRequest();

  protected:
	// std::string path;
};

class AmazonS3CompleteMultipartUpload : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3CompleteMultipartUpload(const S3AccessInfo &ai,
									const std::string &objectName,
									XrdSysError &log)
		: AmazonRequest(ai, objectName, log) {}

	AmazonS3CompleteMultipartUpload(const std::string &s,
									const std::string &akf,
									const std::string &skf,
									const std::string &b, const std::string &o,
									const std::string &style, XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	virtual ~AmazonS3CompleteMultipartUpload();

	virtual bool SendRequest(const std::vector<std::string> &eTags,
							 int partNumber, const std::string &uploadId);

  protected:
};

class AmazonS3SendMultipartPart : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3SendMultipartPart(const S3AccessInfo &ai,
							  const std::string &objectName, XrdSysError &log)
		: AmazonRequest(ai, objectName, log) {}

	AmazonS3SendMultipartPart(const std::string &s, const std::string &akf,
							  const std::string &skf, const std::string &b,
							  const std::string &o, const std::string &style,
							  XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	bool Results(std::string &uploadId, std::string &errMsg);

	virtual ~AmazonS3SendMultipartPart();

	// Send (potentially a partial) payload up to S3.
	// Blocks until all the data in payload has been sent to AWS.
	//
	// - payload: The data corresponding to this partial upload.
	// - partNumber: The portion of the multipart upload.
	// - uploadId: The upload ID assigned by the creation of the multipart
	// upload
	// - final: Set to true if this is the last of the part; false otherwise
	bool SendRequest(const std::string_view payload,
					 const std::string &partNumber, const std::string &uploadId,
					 size_t payloadSize, bool final);

	// Retrieve the ETag header from the returned headers;
	bool GetEtag(std::string &result);

  private:
	std::string m_etag;
};

class AmazonS3Download : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3Download(const S3AccessInfo &ai, const std::string &objectName,
					 XrdSysError &log, char *buffer)
		: AmazonRequest(ai, objectName, log), m_buffer(buffer) {}

	AmazonS3Download(const std::string &s, const std::string &akf,
					 const std::string &skf, const std::string &b,
					 const std::string &o, const std::string &style,
					 XrdSysError &log, char *buffer)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log), m_buffer(buffer) {}

	virtual ~AmazonS3Download();

	virtual bool SendRequest(off_t offset, size_t size);

	// Return the elapsed time since the request was started with SendRequest().
	std::chrono::steady_clock::duration getElapsedTime() const {
		return std::chrono::steady_clock::now() - m_request_start;
	}

  protected:
	virtual bool IsBlocking() { return true; }
	virtual std::string_view *requestResult() override {
		return &m_buffer_view;
	}

  private:
	char *m_buffer{nullptr};
	std::chrono::steady_clock::time_point m_request_start;
	std::string_view m_buffer_view;
};

template <typename T>
class AmazonS3NonblockingDownload final : public AmazonS3Download {

  public:
	AmazonS3NonblockingDownload(const S3AccessInfo &ai,
								const std::string &objectName, XrdSysError &log,
								char *buffer, T &notifier)
		: AmazonS3Download(ai, objectName, log, buffer), m_notifier(notifier) {}

	virtual ~AmazonS3NonblockingDownload();

  protected:
	virtual bool IsBlocking() override { return false; }
	virtual void Notify() override { m_notifier.Notify(); }

  private:
	T &m_notifier;
};

class AmazonS3Head final : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3Head(const S3AccessInfo &ai, const std::string &objectName,
				 XrdSysError &log)
		: AmazonRequest(ai, objectName, log) {}

	AmazonS3Head(const std::string &s, const std::string &akf,
				 const std::string &skf, const std::string &b,
				 const std::string &o, const std::string &style,
				 XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	virtual ~AmazonS3Head();

	virtual bool SendRequest();

	off_t getSize() {
		parseResponse();
		return m_size;
	}
	time_t getLastModified() {
		parseResponse();
		return m_last_modified;
	}

  private:
	void parseResponse();

	bool m_parsedResponse{false};
	off_t m_size{0};
	time_t m_last_modified{0};
};

struct S3ObjectInfo {
	size_t m_size;
	std::string m_key;
};

class AmazonS3List final : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3List(const S3AccessInfo &ai, const std::string &objectName,
				 size_t maxKeys, XrdSysError &log)
		: AmazonRequest(ai, objectName, log, false), m_maxKeys(maxKeys) {}

	virtual ~AmazonS3List() {}

	bool SendRequest(const std::string &continuationToken);
	bool Results(std::vector<S3ObjectInfo> &objInfo,
				 std::vector<std::string> &commonPrefixes, std::string &ct,
				 std::string &errMsg);

  private:
	size_t m_maxKeys{1000};
};
