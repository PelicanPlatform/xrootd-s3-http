/**
 * Utilities for generating pre-signed URLs.
 *
 * These were originally authored by the HTCondor team under the Apache 2.0 license which
 * can also be found in the LICENSE file at the top-level directory of this project.  No
 * copyright statement was present in the original file.
 */

#include <algorithm>
#include <cstring>
#include <openssl/hmac.h>
#include <sstream>
#include <string>

#include "AWSUtils.hh"

namespace {

//
// This function should not be called for anything in query_parameters,
// except for by AmazonQuery::SendRequest().
//
std::string
amazonURLEncode( const std::string & input ) {
    /*
     * See http://docs.amazonwebservices.com/AWSEC2/2010-11-15/DeveloperGuide/using-query-api.html
     *
     */
    std::string output;
    for( unsigned i = 0; i < input.length(); ++i ) {
        // "Do not URL encode ... A-Z, a-z, 0-9, hyphen ( - ),
        // underscore ( _ ), period ( . ), and tilde ( ~ ).  Percent
        // encode all other characters with %XY, where X and Y are hex
        // characters 0-9 and uppercase A-F.  Percent encode extended
        // UTF-8 characters in the form %XY%ZA..."
        if( ('A' <= input[i] && input[i] <= 'Z')
         || ('a' <= input[i] && input[i] <= 'z')
         || ('0' <= input[i] && input[i] <= '9')
         || input[i] == '-'
         || input[i] == '_'
         || input[i] == '.'
         || input[i] == '~' ) {
            char uglyHack[] = "X";
            uglyHack[0] = input[i];
            output.append( uglyHack );
        } else {
            char percentEncode[4];
            snprintf(percentEncode, 4, "%%%.2hhX", input[i]);
            output.append(percentEncode);
        }
    }

    return output;
}


std::string
pathEncode( const std::string & original ) {
	std::string segment;
	std::string encoded;
	const char * o = original.c_str();

	size_t next = 0;
	size_t offset = 0;
	size_t length = strlen( o );
	while( offset < length ) {
		next = strcspn( o + offset, "/" );
		if( next == 0 ) { encoded += "/"; offset += 1; continue; }

		segment = std::string( o + offset, next );
		encoded += amazonURLEncode( segment );

		offset += next;
	}
	return encoded;
}


void
convertMessageDigestToLowercaseHex(
  const unsigned char * messageDigest,
  unsigned int mdLength, std::string & hexEncoded ) {
	char * buffer = (char *)malloc( (mdLength * 2) + 1 );
	char * ptr = buffer;
	for (unsigned int i = 0; i < mdLength; ++i, ptr += 2) {
		sprintf(ptr, "%02x", messageDigest[i]);
	}
	hexEncoded.assign(buffer, mdLength * 2);
	free(buffer);
}

bool
doSha256( const std::string & payload,
  unsigned char * messageDigest,
  unsigned int * mdLength ) {
	EVP_MD_CTX * mdctx = EVP_MD_CTX_create();
	if( mdctx == NULL ) { return false; }

	if(! EVP_DigestInit_ex( mdctx, EVP_sha256(), NULL )) {
		EVP_MD_CTX_destroy( mdctx );
		return false;
	}

	if(! EVP_DigestUpdate( mdctx, payload.c_str(), payload.length() )) {
		EVP_MD_CTX_destroy( mdctx );
		return false;
	}

	if(! EVP_DigestFinal_ex( mdctx, messageDigest, mdLength )) {
		EVP_MD_CTX_destroy( mdctx );
		return false;
	}

	EVP_MD_CTX_destroy( mdctx );
	return true;
}

bool
createSignature( const std::string & secretAccessKey,
  const std::string & date, const std::string & region,
  const std::string & service, const std::string & stringToSign,
  std::string & signature ) {
    unsigned int mdLength = 0;
    unsigned char messageDigest[EVP_MAX_MD_SIZE];

	std::string saKey = "AWS4" + secretAccessKey;
	const unsigned char * hmac = HMAC( EVP_sha256(), saKey.c_str(), saKey.length(),
		(const unsigned char *)date.c_str(), date.length(),
		messageDigest, & mdLength );
	if( hmac == NULL ) { return false; }

	unsigned int md2Length = 0;
	unsigned char messageDigest2[EVP_MAX_MD_SIZE];
	hmac = HMAC( EVP_sha256(), messageDigest, mdLength,
		(const unsigned char *)region.c_str(), region.length(), messageDigest2, & md2Length );
	if( hmac == NULL ) { return false; }

	hmac = HMAC( EVP_sha256(), messageDigest2, md2Length,
		(const unsigned char *)service.c_str(), service.length(), messageDigest, & mdLength );
	if( hmac == NULL ) { return false; }

	const char c[] = "aws4_request";
	hmac = HMAC( EVP_sha256(), messageDigest, mdLength,
		(const unsigned char *)c, sizeof(c) - 1, messageDigest2, & md2Length );
	if( hmac == NULL ) { return false; }

	hmac = HMAC( EVP_sha256(), messageDigest2, md2Length,
		(const unsigned char *)stringToSign.c_str(), stringToSign.length(),
		messageDigest, & mdLength );
	if( hmac == NULL ) { return false; }

	convertMessageDigestToLowercaseHex( messageDigest, mdLength, signature );
	return true;
}

}


bool
AWSCredential::presign(const std::string &input_region,
  const std::string &bucket,
  const std::string &object,
  const std::string &verb,
  std::string &presignedURL,
  std::string &err)
{
    time_t now; time( & now );
    // Allow for modest clock skews.
    now -= 5;
    struct tm brokenDownTime; gmtime_r( & now, & brokenDownTime );
    char dateAndTime[] = "YYYYMMDDThhmmssZ";
    strftime(dateAndTime, sizeof(dateAndTime), "%Y%m%dT%H%M%SZ",
        &brokenDownTime);
    char date[] = "YYYYMMDD";
    strftime(date, sizeof(date), "%Y%m%d", & brokenDownTime);

    // If the named bucket isn't valid as part of a DNS name,
    // we assume it's an old "path-style"-only bucket.
    std::string canonicalURI("/");

    std::string region = input_region;
    std::string host;
    if (region.empty()) {
        host = bucket + ".s3.amazonaws.com";
        region = "us-east-1";
    } else {
        host = bucket + ".s3." + region + ".amazonaws.com";
    }

    //
    // Construct the canonical request.
    //

    // Part 1: The canonical URI.  Note that we don't have to worry about
    // path normalization, because S3 objects aren't actually path names.
    canonicalURI += pathEncode(object);

    // Part 4: The signed headers.
    const std::string signedHeaders = "host";

    //
    // Part 2: The canonical query string.
    //
    const auto service = "s3";
    std::stringstream credss;
    credss << date << "/" << region << "/" << service << "/aws4_request";
    std::string credentialScope = credss.str();

    std::stringstream queryss;
    queryss << "X-Amz-Algorithm=AWS4-HMAC-SHA256&";
    queryss << "X-Amz-Credential=" << m_access_key << "/" << credentialScope << "&";
    queryss << "X-Amz-Date=" << dateAndTime << "&";
    queryss << "X-Amz-Expires=3600&";
    queryss << "X-Amz-SignedHeaders=" << signedHeaders << "&";
    if (!m_security_token.empty()) {
        queryss << "X-Amz-Security-Token=" << m_security_token << "&";
    }
    auto canonicalQueryString = queryss.str();
    canonicalQueryString.erase(canonicalQueryString.end() - 1);

    // Part 3: The canonical headers.  This MUST include "Host".
    std::stringstream hdrss;
    hdrss << "host:" << host << "\n";
    auto canonicalHeaders = hdrss.str();

    std::string canonicalRequest = verb + "\n"
                                 + canonicalURI + "\n"
                                 + canonicalQueryString + "\n"
                                 + canonicalHeaders + "\n"
                                 + signedHeaders + "\n"
                                 + "UNSIGNED-PAYLOAD";

    //
    // Create the signature.
    //
    unsigned int mdLength = 0;
    unsigned char messageDigest[EVP_MAX_MD_SIZE];
    std::string canonicalRequestHash;
    if (!doSha256(canonicalRequest, messageDigest, &mdLength)) {
        err = "unable to hash canonical request, failing";
        return false;
    }
    convertMessageDigestToLowercaseHex(messageDigest, mdLength, canonicalRequestHash);

    std::stringstream signss;
    signss << "AWS4-HMAC-SHA256\n" << dateAndTime << "\n" << credentialScope << "\n" << canonicalRequestHash;
    std::string stringToSign;

    std::string signature;
    if (!createSignature(m_access_key, date, region, service, stringToSign, signature)) {
        err = "failed to create signature";
        return false;
    }

    std::stringstream finalss;
    finalss << "https://" << host << canonicalURI << "?" << canonicalQueryString
            << "&X-Amz-Signature=" << signature;
    signature = finalss.str();

    return true;
}
