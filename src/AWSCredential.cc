#include <map>
#include <string>
#include "AWSv4-impl.hh"

#include "AWSCredential.hh"

#include <openssl/hmac.h>
#include <sstream>

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
    canonicalURI += AWSv4Impl::pathEncode(object);

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
    if (!AWSv4Impl::doSha256(canonicalRequest, messageDigest, &mdLength)) {
        err = "unable to hash canonical request, failing";
        return false;
    }
    AWSv4Impl::convertMessageDigestToLowercaseHex(messageDigest, mdLength, canonicalRequestHash);

    std::stringstream signss;
    signss << "AWS4-HMAC-SHA256\n" << dateAndTime << "\n" << credentialScope << "\n" << canonicalRequestHash;
    std::string stringToSign;

    std::string signature;
    if (!AWSv4Impl::createSignature(m_access_key, date, region, service, stringToSign, signature)) {
        err = "failed to create signature";
        return false;
    }

    std::stringstream finalss;
    finalss << "https://" << host << canonicalURI << "?" << canonicalQueryString
            << "&X-Amz-Signature=" << signature;
    signature = finalss.str();

    return true;
}
