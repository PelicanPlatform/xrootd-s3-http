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

#include <string>

class AmazonRequest : public HTTPRequest {
  public:
	AmazonRequest(const std::string &s, const std::string &akf,
				  const std::string &skf, const std::string &b,
				  const std::string &o, const std::string &style, int sv,
				  XrdSysError &log)
		: HTTPRequest(s, log), accessKeyFile(akf), secretKeyFile(skf),
		  signatureVersion(sv), bucket(b), object(o), style(style) {
		requiresSignature = true;
		// Start off by parsing the hostUrl, which we use in conjunction with
		// the bucket to fill in the host (for setting host header). For
		// example, if the incoming hostUrl (which we get from config) is
		// "https://my-url.com:443", the bucket is "my-bucket", and the object
		// is "my-object", then the host will be "my-bucket.my-url.com:443" and
		// the canonicalURI will be "/my-object".
		if (!parseURL(hostUrl, canonicalURI)) {
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
		hostUrl = protocol + "://" + host + canonicalURI;

		// If we can, set the region based on the host.
		size_t secondDot = host.find(".", 2 + 1);
		if (host.find("s3.") == 0) {
			region = host.substr(3, secondDot - 2 - 1);
		}
	}
	virtual ~AmazonRequest();

	virtual const std::string *getAccessKey() const { return &accessKeyFile; }
	virtual const std::string *getSecretKey() const { return &secretKeyFile; }

	bool parseURL(const std::string &url, std::string &path);

	virtual bool SendRequest();
	virtual bool SendS3Request(const std::string &payload);

  protected:
	bool sendV4Request(const std::string &payload, bool sendContentSHA = false);

	std::string accessKeyFile;
	std::string secretKeyFile;

	int signatureVersion;

	std::string host;
	std::string canonicalURI;

	std::string bucket;
	std::string object;

	std::string region;
	std::string service;

	std::string style;

  private:
	bool createV4Signature(const std::string &payload,
						   std::string &authorizationHeader,
						   bool sendContentSHA = false);

	std::string canonicalizeQueryString();
};

class AmazonS3Upload : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3Upload(const std::string &s, const std::string &akf,
				   const std::string &skf, const std::string &b,
				   const std::string &o, const std::string &style,
				   XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	virtual ~AmazonS3Upload();

	virtual bool SendRequest(const std::string &payload, off_t offset,
							 size_t size);

  protected:
	std::string path;
};

class AmazonS3Download : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3Download(const std::string &s, const std::string &akf,
					 const std::string &skf, const std::string &b,
					 const std::string &o, const std::string &style,
					 XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	virtual ~AmazonS3Download();

	virtual bool SendRequest(off_t offset, size_t size);
};

class AmazonS3Head : public AmazonRequest {
	using AmazonRequest::SendRequest;

  public:
	AmazonS3Head(const std::string &s, const std::string &akf,
				 const std::string &skf, const std::string &b,
				 const std::string &o, const std::string &style,
				 XrdSysError &log)
		: AmazonRequest(s, akf, skf, b, o, style, 4, log) {}

	virtual ~AmazonS3Head();

	virtual bool SendRequest();
};
