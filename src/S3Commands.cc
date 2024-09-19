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

#include "S3Commands.hh"
#include "AWSv4-impl.hh"
#include "shortfile.hh"
#include "stl_string_utils.hh"

#include <XrdSys/XrdSysError.hh>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <tinyxml2.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <string>

AmazonRequest::~AmazonRequest() {}

bool AmazonRequest::SendRequest() {
	query_parameters.insert(std::make_pair("Version", "2012-10-01"));

	switch (signatureVersion) {
	case 4:
		return sendV4Request(canonicalizeQueryString());
	default:
		this->errorCode = "E_INTERNAL";
		this->errorMessage = "Invalid signature version.";
		return false;
	}
}

std::string AmazonRequest::canonicalizeQueryString() {
	return AWSv4Impl::canonicalizeQueryString(query_parameters);
}

// Takes in the configured `s3.service_url` and uses the bucket/object requested
// to generate the host URL, as well as the canonical URI (which is the path to
// the object).
bool AmazonRequest::parseURL(const std::string &url, std::string &bucket_path,
							 std::string &path) {
	auto schemeEndIdx = url.find("://");
	if (schemeEndIdx == std::string::npos) {
		return false;
	}
	if (url.size() < schemeEndIdx + 3) {
		return false;
	}
	auto hostStartIdx = schemeEndIdx + 3;

	auto resourceStartIdx = url.find("/", hostStartIdx);
	if (resourceStartIdx == std::string::npos) {
		if (m_style == "path") {
			// If we're configured for path-style requests, then the host is
			// everything between
			// :// and the last /
			host = substring(url, hostStartIdx);
			// Likewise, the path is going to be /bucket/object
			// Sometimes we intentionally configure the plugin with no bucket
			// because we assume the incoming object request already encodes the
			// bucket. This is used for exporting many buckets from a single
			// endpoint.
			if (bucket.empty()) {
				path = "/" + object;
				bucket_path = "/" + object.substr(0, object.find('/'));
			} else {
				path = "/" + bucket + "/" + object;
				bucket_path = "/" + bucket;
			}
		} else {
			// In virtual-style requests, the host should be determined as
			// everything between
			// :// up until the last /, but with <bucket> appended to the front.
			host = bucket + "." + substring(url, hostStartIdx);
			if (retain_object) {
				path = "/" + object;
			} else {
				path = "/";
			}

			bucket_path = "/";
		}

		return true;
	}

	if (m_style == "path") {
		host = substring(url, hostStartIdx, resourceStartIdx);
		auto resourcePrefix = substring(url, resourceStartIdx);
		if (resourcePrefix[resourcePrefix.size() - 1] == '/') {
			resourcePrefix =
				substring(resourcePrefix, 0, resourcePrefix.size() - 1);
		}
		if (bucket.empty()) {
			path = resourcePrefix + object;
			bucket_path = resourcePrefix + object.substr(0, object.find('/'));
		} else {
			path = resourcePrefix + "/" + bucket + "/" + object;
			bucket_path = resourcePrefix + "/" + bucket;
		}
	} else {
		host = bucket + "." + substring(url, hostStartIdx, resourceStartIdx);
		path = substring(url, resourceStartIdx) + object;
		bucket_path = "/";
	}

	return true;
}

void convertMessageDigestToLowercaseHex(const unsigned char *messageDigest,
										unsigned int mdLength,
										std::string &hexEncoded) {
	AWSv4Impl::convertMessageDigestToLowercaseHex(messageDigest, mdLength,
												  hexEncoded);
}

bool doSha256(const std::string &payload, unsigned char *messageDigest,
			  unsigned int *mdLength) {
	return AWSv4Impl::doSha256(payload, messageDigest, mdLength);
}

std::string pathEncode(const std::string &original) {
	return AWSv4Impl::pathEncode(original);
}

bool AmazonRequest::createV4Signature(const std::string &payload,
									  std::string &authorizationValue,
									  bool sendContentSHA) {
	// If we're using temporary credentials, we need to add the token
	// header here as well.  We set saKey and keyID here (well before
	// necessary) since we'll get them for free when we get the token.
	std::string keyID;
	std::string saKey;
	std::string token;
	if (!this->secretKeyFile.empty()) { // Some origins may exist in front of
										// unauthenticated buckets
		if (!readShortFile(this->secretKeyFile, saKey)) {
			this->errorCode = "E_FILE_IO";
			this->errorMessage = "Unable to read from secretkey file '" +
								 this->secretKeyFile + "'.";
			return false;
		}
		trim(saKey);
	} else {
		canonicalQueryString = canonicalizeQueryString();

		requiresSignature =
			false;	 // If we don't create a signature, it must not be needed...
		return true; // If there was no saKey, we need not generate a signature
	}

	if (!this->accessKeyFile.empty()) { // Some origins may exist in front of
										// unauthenticated buckets
		if (!readShortFile(this->accessKeyFile, keyID)) {
			this->errorCode = "E_FILE_IO";
			this->errorMessage = "Unable to read from accesskey file '" +
								 this->accessKeyFile + "'.";
			return false;
		}
		trim(keyID);
	} else {
		this->errorCode = "E_FILE_IO";
		this->errorMessage = "The secretkey file was read, but I can't read "
							 "from accesskey file '" +
							 this->secretKeyFile + "'.";
		return false;
	}

	time_t now;
	time(&now);
	struct tm brokenDownTime;
	gmtime_r(&now, &brokenDownTime);

	//
	// Create task 1's inputs.
	//

	// The canonical URI is the absolute path component of the service URL,
	// normalized according to RFC 3986 (removing redundant and relative
	// path components), with each path segment being URI-encoded.

	// But that sounds like a lot of work, so until something we do actually
	// requires it, I'll just assume the path is already normalized.
	canonicalURI = pathEncode(canonicalURI);

	// The canonical query string is the alphabetically sorted list of
	// URI-encoded parameter names '=' values, separated by '&'s.
	canonicalQueryString = canonicalizeQueryString();

	// The canonical headers must include the Host header, so add that
	// now if we don't have it.
	if (headers.find("Host") == headers.end()) {
		headers["Host"] = host;
	}

	// S3 complains if x-amz-date isn't signed, so do this early.
	char dt[] = "YYYYMMDDThhmmssZ";
	strftime(dt, sizeof(dt), "%Y%m%dT%H%M%SZ", &brokenDownTime);
	headers["X-Amz-Date"] = dt;

	char d[] = "YYYYMMDD";
	strftime(d, sizeof(d), "%Y%m%d", &brokenDownTime);

	// S3 complains if x-amz-content-sha256 isn't signed, which makes sense,
	// so do this early.

	// The canonical payload hash is the lowercase hexadecimal string of the
	// (SHA256) hash value of the payload.
	unsigned int mdLength = 0;
	unsigned char messageDigest[EVP_MAX_MD_SIZE];
	if (!doSha256(payload, messageDigest, &mdLength)) {
		this->errorCode = "E_INTERNAL";
		this->errorMessage = "Unable to hash payload.";
		return false;
	}
	std::string payloadHash;
	convertMessageDigestToLowercaseHex(messageDigest, mdLength, payloadHash);
	if (sendContentSHA) {
		headers["X-Amz-Content-Sha256"] = payloadHash;
	}

	// The canonical list of headers is a sorted list of lowercase header
	// names paired via ':' with the trimmed header value, each pair
	// terminated with a newline.
	AmazonRequest::AttributeValueMap transformedHeaders;
	for (auto i = headers.begin(); i != headers.end(); ++i) {
		std::string header = i->first;
		std::transform(header.begin(), header.end(), header.begin(), &tolower);

		std::string value = i->second;
		// We need to leave empty headers alone so that they can be used
		// to disable CURL stupidity later.
		if (value.size() == 0) {
			continue;
		}

		// Eliminate trailing spaces.
		unsigned j = value.length() - 1;
		while (value[j] == ' ') {
			--j;
		}
		if (j != value.length() - 1) {
			value.erase(j + 1);
		}

		// Eliminate leading spaces.
		for (j = 0; value[j] == ' '; ++j) {
		}
		value.erase(0, j);

		// Convert internal runs of spaces into single spaces.
		unsigned left = 1;
		unsigned right = 1;
		bool inSpaces = false;
		while (right < value.length()) {
			if (!inSpaces) {
				if (value[right] == ' ') {
					inSpaces = true;
					left = right;
					++right;
				} else {
					++right;
				}
			} else {
				if (value[right] == ' ') {
					++right;
				} else {
					inSpaces = false;
					value.erase(left, right - left - 1);
					right = left + 1;
				}
			}
		}

		transformedHeaders[header] = value;
	}

	// The canonical list of signed headers is trivial to generate while
	// generating the list of headers.
	std::string signedHeaders;
	std::string canonicalHeaders;
	for (auto i = transformedHeaders.begin(); i != transformedHeaders.end();
		 ++i) {
		canonicalHeaders += i->first + ":" + i->second + "\n";
		signedHeaders += i->first + ";";
	}
	signedHeaders.erase(signedHeaders.end() - 1);

	// Task 1: create the canonical request.
	std::string canonicalRequest =
		httpVerb + "\n" + canonicalURI + "\n" + canonicalQueryString + "\n" +
		canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash;

	//
	// Create task 2's inputs.
	//

	// Hash the canonical request the way we did the payload.
	if (!doSha256(canonicalRequest, messageDigest, &mdLength)) {
		this->errorCode = "E_INTERNAL";
		this->errorMessage = "Unable to hash canonical request.";
		return false;
	}
	std::string canonicalRequestHash;
	convertMessageDigestToLowercaseHex(messageDigest, mdLength,
									   canonicalRequestHash);

	std::string s = this->service;
	if (s.empty()) {
		size_t i = host.find(".");
		if (i != std::string::npos) {
			s = host.substr(0, i);
		} else {
			s = host;
		}
	}

	std::string r = this->region;
	if (r.empty()) {
		size_t i = host.find(".");
		size_t j = host.find(".", i + 1);
		if (j != std::string::npos) {
			r = host.substr(i + 1, j - i - 1);
		} else {
			r = host;
		}
	}

	// Task 2: create the string to sign.
	std::string credentialScope;
	formatstr(credentialScope, "%s/%s/%s/aws4_request", d, r.c_str(),
			  s.c_str());
	std::string stringToSign;
	formatstr(stringToSign, "AWS4-HMAC-SHA256\n%s\n%s\n%s", dt,
			  credentialScope.c_str(), canonicalRequestHash.c_str());

	//
	// Creating task 3's inputs was done when we checked to see if we needed
	// to get the security token, since they come along for free when we do.
	//

	// Task 3: calculate the signature.
	saKey = "AWS4" + saKey;
	const unsigned char *hmac =
		HMAC(EVP_sha256(), saKey.c_str(), saKey.length(), (unsigned char *)d,
			 sizeof(d) - 1, messageDigest, &mdLength);
	if (hmac == NULL) {
		return false;
	}

	unsigned int md2Length = 0;
	unsigned char messageDigest2[EVP_MAX_MD_SIZE];
	hmac = HMAC(EVP_sha256(), messageDigest, mdLength,
				(const unsigned char *)r.c_str(), r.length(), messageDigest2,
				&md2Length);
	if (hmac == NULL) {
		return false;
	}

	hmac = HMAC(EVP_sha256(), messageDigest2, md2Length,
				(const unsigned char *)s.c_str(), s.length(), messageDigest,
				&mdLength);
	if (hmac == NULL) {
		return false;
	}

	const char c[] = "aws4_request";
	hmac = HMAC(EVP_sha256(), messageDigest, mdLength, (const unsigned char *)c,
				sizeof(c) - 1, messageDigest2, &md2Length);
	if (hmac == NULL) {
		return false;
	}

	hmac = HMAC(EVP_sha256(), messageDigest2, md2Length,
				(const unsigned char *)stringToSign.c_str(),
				stringToSign.length(), messageDigest, &mdLength);
	if (hmac == NULL) {
		return false;
	}

	std::string signature;
	convertMessageDigestToLowercaseHex(messageDigest, mdLength, signature);

	formatstr(authorizationValue,
			  "AWS4-HMAC-SHA256 Credential=%s/%s,"
			  " SignedHeaders=%s, Signature=%s",
			  keyID.c_str(), credentialScope.c_str(), signedHeaders.c_str(),
			  signature.c_str());
	return true;
}

bool AmazonRequest::sendV4Request(const std::string &payload,
								  bool sendContentSHA) {
	if ((protocol != "http") && (protocol != "https")) {
		this->errorCode = "E_INVALID_SERVICE_URL";
		this->errorMessage = "Service URL not of a known protocol (http[s]).";
		return false;
	}

	std::string authorizationValue;
	if (!createV4Signature(payload, authorizationValue, sendContentSHA)) {
		if (this->errorCode.empty()) {
			this->errorCode = "E_INTERNAL";
		}
		if (this->errorMessage.empty()) {
			this->errorMessage = "Failed to create v4 signature.";
		}
		return false;
	}

	// When accessing an unauthenticated bucket, providing an auth header will
	// cause errors
	if (!authorizationValue.empty()) {
		headers["Authorization"] = authorizationValue;
	}

	// This operation is on the bucket itself; alter the URL
	auto url = hostUrl;
	if (!canonicalQueryString.empty()) {
		url += "?" + canonicalQueryString;
	}
	return sendPreparedRequest(protocol, url, payload);
}

// It's stated in the API documentation that you can upload to any region
// via us-east-1, which is moderately crazy.
bool AmazonRequest::SendS3Request(const std::string &payload) {
	headers["Content-Type"] = "binary/octet-stream";
	std::string contentLength;
	formatstr(contentLength, "%zu", payload.size());
	headers["Content-Length"] = contentLength;
	// Another undocumented CURL feature: transfer-encoding is "chunked"
	// by default for "PUT", which we really don't want.
	headers["Transfer-Encoding"] = "";
	service = "s3";
	if (region.empty()) {
		region = "us-east-1";
	}
	return sendV4Request(payload, true);
}

// ---------------------------------------------------------------------------

AmazonS3Upload::~AmazonS3Upload() {}

bool AmazonS3Upload::SendRequest(const std::string &payload, off_t offset,
								 size_t size) {
	if (offset != 0 || size != 0) {
		std::string range;
		formatstr(range, "bytes=%lld-%lld", static_cast<long long int>(offset),
				  static_cast<long long int>(offset + size - 1));
		headers["Range"] = range.c_str();
	}

	httpVerb = "PUT";
	return SendS3Request(payload);
}

// ---------------------------------------------------------------------------

AmazonS3CompleteMultipartUpload::~AmazonS3CompleteMultipartUpload() {}

bool AmazonS3CompleteMultipartUpload::SendRequest(
	const std::vector<std::string> &eTags, int partNumber,
	const std::string &uploadId) {
	query_parameters["uploadId"] = uploadId;

	httpVerb = "POST";
	std::string payload;
	payload += "<CompleteMultipartUpload "
			   "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
	for (int i = 1; i < partNumber; i++) {
		payload += "<Part>";
		payload += "<ETag>" + eTags[i - 1] + "</ETag>";
		payload += "<PartNumber>" + std::to_string(i) + "</PartNumber>";
		payload += "</Part>";
	}
	payload += "</CompleteMultipartUpload>";

	return SendS3Request(payload);
}
// ---------------------------------------------------------------------------

AmazonS3CreateMultipartUpload::~AmazonS3CreateMultipartUpload() {}
AmazonS3SendMultipartPart::~AmazonS3SendMultipartPart() {}

bool AmazonS3CreateMultipartUpload::SendRequest() {
	query_parameters["uploads"] = "";
	query_parameters["x-id"] = "CreateMultipartUpload";

	httpVerb = "POST";
	return SendS3Request("");
}

bool AmazonS3SendMultipartPart::SendRequest(const std::string &payload,
											const std::string &partNumber,
											const std::string &uploadId) {
	query_parameters["partNumber"] = partNumber;
	query_parameters["uploadId"] = uploadId;
	includeResponseHeader = true;
	httpVerb = "PUT";
	return SendS3Request(payload);
}

// ---------------------------------------------------------------------------

AmazonS3Download::~AmazonS3Download() {}

bool AmazonS3Download::SendRequest(off_t offset, size_t size) {
	if (offset != 0 || size != 0) {
		std::string range;
		formatstr(range, "bytes=%lld-%lld", static_cast<long long int>(offset),
				  static_cast<long long int>(offset + size - 1));
		headers["Range"] = range.c_str();
		this->expectedResponseCode = 206;
	}

	httpVerb = "GET";
	std::string noPayloadAllowed;
	return SendS3Request(noPayloadAllowed);
}

// ---------------------------------------------------------------------------

AmazonS3Head::~AmazonS3Head() {}

bool AmazonS3Head::SendRequest() {
	httpVerb = "HEAD";
	includeResponseHeader = true;
	std::string noPayloadAllowed;
	return SendS3Request(noPayloadAllowed);
}

// ---------------------------------------------------------------------------

bool AmazonS3List::SendRequest(const std::string &continuationToken) {
	query_parameters["list-type"] = "2"; // Version 2 of the object-listing API
	query_parameters["delimiter"] = "/";
	query_parameters["prefix"] = urlquote(object);
	if (!continuationToken.empty()) {
		query_parameters["continuation-token"] = urlquote(continuationToken);
	}
	query_parameters["max-keys"] = std::to_string(m_maxKeys);
	httpVerb = "GET";

	// Operation is on the bucket itself; alter the URL to remove the object
	hostUrl = protocol + "://" + host + bucketPath;

	return SendS3Request("");
}

bool AmazonS3CreateMultipartUpload::Results(std::string &uploadId,
											std::string &errMsg) {
	tinyxml2::XMLDocument doc;
	auto err = doc.Parse(resultString.c_str());
	if (err != tinyxml2::XML_SUCCESS) {
		errMsg = doc.ErrorStr();
		return false;
	}

	auto elem = doc.RootElement();
	if (strcmp(elem->Name(), "InitiateMultipartUploadResult")) {
		errMsg = "S3 Uploads response is not rooted with "
				 "InitiateMultipartUploadResult "
				 "element";
		return false;
	}

	for (auto child = elem->FirstChildElement(); child != nullptr;
		 child = child->NextSiblingElement()) {
		if (!strcmp(child->Name(), "UploadId")) {
			uploadId = child->GetText();
		}
	}
	return true;
}

// Parse the results of the AWS directory listing
//
// S3 returns an XML structure for directory listings so we must pick it apart
// and convert it to `objInfo` and `commonPrefixes`.  The `objInfo` is a list of
// objects that match the current prefix but don't have a subsequent `/` in the
// object name. The `commonPrefixes` are the unique prefixes of other objects
// that have the same prefix as the original query but also have an `/`.
//
// Example.  Suppose we have the following objects in the bucket:
// - /foo/bar.txt
// - /foo/bar/example.txt
// - /foo/baz/example.txt
// Then, a query to list with prefix `/foo/` would return object info for
// `/foo/bar.txt` while the common prefixes would be `/foo/bar/` and `/foo/baz`.
// Note this is quite close to returning a list of files in a directory and a
// list of sub-directories.
bool AmazonS3List::Results(std::vector<S3ObjectInfo> &objInfo,
						   std::vector<std::string> &commonPrefixes,
						   std::string &ct, std::string &errMsg) {
	tinyxml2::XMLDocument doc;
	auto err = doc.Parse(resultString.c_str());
	if (err != tinyxml2::XML_SUCCESS) {
		errMsg = doc.ErrorStr();
		return false;
	}

	auto elem = doc.RootElement();
	if (strcmp(elem->Name(), "ListBucketResult")) {
		errMsg = "S3 ListBucket response is not rooted with ListBucketResult "
				 "element";
		return false;
	}

	// Example response from S3:
	// <?xml version="1.0" encoding="utf-8"?>
	// <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	//   <Name>genome-browser</Name>
	//   <Prefix>cells/muscle-ibm/endothelial-stromal-cells</Prefix>
	//   <KeyCount>40</KeyCount>
	//   <MaxKeys>40</MaxKeys>
	//   <NextContinuationToken>1PnsptbFFpBSb6UBNN4F/RrxtBvIHjNpdXNYlX8E7IyqXRK26w2y36KViUAbyPPsjzikVY0Zj4jMvQHRhsGWZbcKKrEVvaR0HaZDtfUXUwnc=</NextContinuationToken>
	// <IsTruncated>false</IsTruncated>
	//   <Contents>
	//     <Key>cells/muscle-ibm/endothelial-stromal-cells/UMAP.coords.tsv.gz</Key>
	//     <LastModified>2023-08-21T11:02:53.000Z</LastModified>
	//     <ETag>"b9b0065f10cbd91c9d341acc235c63b0"</ETag>
	//     <Size>360012</Size>
	//     <StorageClass>STANDARD</StorageClass>
	//   </Contents>
	//   <Contents>
	//     <Key>cells/muscle-ibm/endothelial-stromal-cells/barcodes.tsv.gz</Key>
	//     <LastModified>2023-07-17T11:02:19.000Z</LastModified>
	//     <ETag>"048feef5d340e2dd4d2d2d495c24ad7e"</ETag>
	//     <Size>118061</Size>
	//     <StorageClass>STANDARD</StorageClass>
	//   </Contents>
	// ... (truncated some entries for readability) ...
	//   <CommonPrefixes>
	//     <Prefix>cells/muscle-ibm/endothelial-stromal-cells/coords/</Prefix>
	//   </CommonPrefixes>
	//   <CommonPrefixes>
	//     <Prefix>cells/muscle-ibm/endothelial-stromal-cells/markers/</Prefix>
	//   </CommonPrefixes>
	//  <CommonPrefixes>
	//    <Prefix>cells/muscle-ibm/endothelial-stromal-cells/metaFields/</Prefix>
	//  </CommonPrefixes>
	// </ListBucketResult>
	bool isTruncated = false;
	for (auto child = elem->FirstChildElement(); child != nullptr;
		 child = child->NextSiblingElement()) {
		if (!strcmp(child->Name(), "IsTruncated")) {
			bool isTrunc;
			if (child->QueryBoolText(&isTrunc) == tinyxml2::XML_SUCCESS) {
				isTruncated = isTrunc;
			}
		} else if (!strcmp(child->Name(), "CommonPrefixes")) {
			auto prefix = child->FirstChildElement("Prefix");
			if (prefix != nullptr) {
				auto prefixChar = prefix->GetText();
				if (prefixChar != nullptr) {
					auto prefixStr = std::string(prefixChar);
					trim(prefixStr);
					if (!prefixStr.empty()) {
						commonPrefixes.emplace_back(prefixStr);
					}
				}
			}
		} else if (!strcmp(child->Name(), "Contents")) {
			std::string keyStr;
			int64_t size;
			bool goodSize = false;
			auto key = child->FirstChildElement("Key");
			if (key != nullptr) {
				auto keyChar = key->GetText();
				if (keyChar != nullptr) {
					keyStr = std::string(keyChar);
					trim(keyStr);
				}
			}
			auto sizeElem = child->FirstChildElement("Size");
			if (sizeElem != nullptr) {
				goodSize =
					(sizeElem->QueryInt64Text(&size) == tinyxml2::XML_SUCCESS);
			}
			if (goodSize && !keyStr.empty()) {
				S3ObjectInfo obj;
				obj.m_key = keyStr;
				obj.m_size = size;
				objInfo.emplace_back(obj);
			}
		} else if (!strcmp(child->Name(), "NextContinuationToken")) {
			auto ctChar = child->GetText();
			if (ctChar) {
				ct = ctChar;
				trim(ct);
			}
		}
	}
	if (!isTruncated) {
		ct = "";
	}
	return true;
}
