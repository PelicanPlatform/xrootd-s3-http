#include <sstream>
#include <algorithm>
#include <openssl/hmac.h>
#include <curl/curl.h>
#include <cassert>
#include <cstring>
#include <memory>

#include <map>
#include <string>

#include "s3Commands.hh"
#include "AWSv4-impl.hh"
#include "stl_string_utils.hh"
#include "shortfile.hh"

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
size_t appendToString( const void * ptr, size_t size, size_t nmemb, void * str ) {
    if( size == 0 || nmemb == 0 ) { return 0; }

    std::string source( (const char *)ptr, size * nmemb );
    std::string * ssptr = (std::string *)str;
    ssptr->append( source );

    return (size * nmemb);
}

AmazonRequest::~AmazonRequest() { }

#define SET_CURL_SECURITY_OPTION( A, B, C ) { \
    CURLcode rv##B = curl_easy_setopt( A, B, C ); \
    if( rv##B != CURLE_OK ) { \
        this->errorCode = "E_CURL_LIB"; \
        this->errorMessage = "curl_easy_setopt( " #B " ) failed."; \
        /* dprintf( D_ALWAYS, "curl_easy_setopt( %s ) failed (%d): '%s', failing.\n", \
            #B, rv##B, curl_easy_strerror( rv##B ) ); */ \
        return false; \
    } \
}


class Throttle {
    public:
        Throttle() : count( 0 ), rateLimit( 0 ) {
#ifdef UNIX
            // Determine which type of clock to use.
            int rv = clock_gettime( CLOCK_MONOTONIC, & when );
            if( rv == 0 ) { type = CLOCK_MONOTONIC; }
            else {
                assert( errno == EINVAL );
                type = CLOCK_REALTIME;
            }
#endif

            when.tv_sec = 0;
            when.tv_nsec = 0;

            deadline.tv_sec = 0;
            deadline.tv_nsec = 0;
        }

        struct timespec getWhen() { return when; }

        // This function is called without the big mutex.  Do NOT add
        // dprintf() statements or refers to globals other than 'this'.
        bool isValid() const { return when.tv_sec != 0; }

        // This function is called without the big mutex.  Do NOT add
        // dprintf() statements or refers to globals other than 'this'.
        struct timespec getDeadline() { return deadline; }

        // This function is called without the big mutex.  Do NOT add
        // dprintf() statements or refers to globals other than 'this'.
        bool setDeadline( struct timespec t, time_t offset ) {
            if( t.tv_sec == 0 ) { return false; }

            deadline = t;
            deadline.tv_sec += offset;
            return true;
        }

        // This function is called without the big mutex.  Do NOT add
        // dprintf() statements or refers to globals other than 'this'.
        void sleepIfNecessary() {
            if( this->isValid() ) {
                int rv;
#if defined(HAVE_CLOCK_NANOSLEEP)
                do {
                    rv = clock_nanosleep( type, TIMER_ABSTIME, & when, NULL );
                } while( rv == EINTR );
#else
                struct timespec delay;
                now( &delay );
                delay.tv_sec = when.tv_sec - delay.tv_sec;
                delay.tv_nsec = when.tv_nsec - delay.tv_nsec;
                if ( delay.tv_nsec < 0 ) {
                    delay.tv_sec -= 1;
                    delay.tv_nsec += 1000000000;
                }
                if ( delay.tv_sec < 0 ) {
                    return;
                }
                do {
                    rv = nanosleep( &delay, &delay );
                } while ( rv != 0 && errno == EINTR );
#endif
                // Suicide rather than overburden the service.
                assert( rv == 0 );
            }
        }

        static void now( struct timespec * t ) {
            if( t != NULL ) {
#ifdef UNIX
                clock_gettime( type, t );
#else
                struct timeval tv;
                gettimeofday( &tv, NULL );
                t->tv_sec = tv.tv_sec;
                t->tv_nsec = tv.tv_usec * 1000;
#endif
            }
        }

        static long difference( const struct timespec * s, const struct timespec * t ) {
            long secondsDiff = t->tv_sec - s->tv_sec;
            long millisDiff = ((t->tv_nsec - s->tv_nsec) + 500000)/1000000;
			// If secondsDiff is too large (as when, for instance, the
			// the liveline is 0 because the limit has never been exceeded
			// and the deadline based on the monotonic clock of a mchine
			// up for more than ~27 days), converting secondsDiff into
			// milliseconds will overflow on 32-bit machines.
			//
			// Since difference() is only called for debugging purposes
			// and to compare against zero, we only care about making
			// sure the sign doesn't change.
			long secondsDiffInMillis = secondsDiff * 1000;
			if( (secondsDiffInMillis < 0 && secondsDiff > 0)
			   || (secondsDiffInMillis > 0 && secondsDiff < 0) ) {
				return secondsDiff;
			}
            millisDiff += (secondsDiff * 1000);
            return millisDiff;
        }

        bool limitExceeded() {
            // Compute until when to sleep before making another request.
            now( & when );

            assert( count < 32 );
            unsigned milliseconds = (1 << count) * 100;
            if( rateLimit > 0 && milliseconds < (unsigned)rateLimit ) { milliseconds = rateLimit; }
            unsigned seconds = milliseconds / 1000;
            unsigned nanoseconds = (milliseconds % 1000) * 1000000;

            // dprintf( D_PERF_TRACE | D_VERBOSE, "limitExceeded(): setting when to %u milliseconds from now (%ld.%09ld)\n", milliseconds, when.tv_sec, when.tv_nsec );
            when.tv_sec += seconds;
            when.tv_nsec += nanoseconds;
            if( when.tv_nsec > 1000000000 ) {
                when.tv_nsec -= 1000000000;
                when.tv_sec += 1;
            }
            // dprintf( D_PERF_TRACE | D_VERBOSE, "limitExceeded(): set when to %ld.%09ld\n", when.tv_sec, when.tv_nsec );

            // If we've waited so long that our next delay will be after
            // the deadline, fail immediately rather than get an invalid
            // signature error.
            if( deadline.tv_sec < when.tv_sec ||
                (deadline.tv_sec == when.tv_sec &&
                  deadline.tv_nsec < when.tv_nsec) ) {
                // dprintf( D_PERF_TRACE | D_VERBOSE, "limitExceeded(): which is after the deadline.\n" );
                return true;
            }

            // Even if we're given a longer deadline, assume that 204.8
            // seconds is as long as we ever want to wait.
            if( count <= 11 ) { ++count; return false; }
            else { return true; }
        }

        void limitNotExceeded() {
            count = 0;
            when.tv_sec = 0;
            deadline.tv_sec = 0;

            if( rateLimit <= 0 ) { return; }

            now( & when );
            unsigned milliseconds = rateLimit;
            unsigned seconds = milliseconds / 1000;
            unsigned nanoseconds = (milliseconds % 1000) * 1000000;
            // dprintf( D_PERF_TRACE | D_VERBOSE, "rate limiting: setting when to %u milliseconds from now (%ld.%09ld)\n", milliseconds, when.tv_sec, when.tv_nsec );

            when.tv_sec += seconds;
            when.tv_nsec += nanoseconds;
            if( when.tv_nsec > 1000000000 ) {
                when.tv_nsec -= 1000000000;
                when.tv_sec += 1;
            }
            // dprintf( D_PERF_TRACE | D_VERBOSE, "rate limiting: set when to %ld.%09ld\n", when.tv_sec, when.tv_nsec );
        }

    protected:
#ifdef UNIX
        static clockid_t type;
#endif
        unsigned int count;
        struct timespec when;
        struct timespec deadline;

	public:
        int rateLimit;

};

#ifdef UNIX
clockid_t Throttle::type;
#endif
Throttle globalCurlThrottle;

pthread_mutex_t globalCurlMutex = PTHREAD_MUTEX_INITIALIZER;

bool AmazonRequest::SendRequest() {
    query_parameters.insert( std::make_pair( "Version", "2012-10-01" ) );

	switch( signatureVersion ) {
		case 4:
			return sendV4Request( canonicalizeQueryString() );
		default:
			this->errorCode = "E_INTERNAL";
			this->errorMessage = "Invalid signature version.";
			// dprintf( D_ALWAYS, "Invalid signature version (%d), failing.\n", signatureVersion );
			return false;
	}
}

std::string AmazonRequest::canonicalizeQueryString() {
    return AWSv4Impl::canonicalizeQueryString( query_parameters );
}

bool parseURL(	const std::string & url,
				std::string & protocol,
				std::string & host,
				std::string & path ) {
    auto i = url.find( "://" );
    if( i == std::string::npos ) { return false; }
    protocol = substring( url, 0, i );

    auto j = url.find( "/", i + 3 );
    if( j == std::string::npos ) {
        host = substring( url, i + 3 );
        path = "/";
        return true;
    }

    host = substring( url, i + 3, j );
    path = substring( url, j );
    return true;
}

void convertMessageDigestToLowercaseHex(
		const unsigned char * messageDigest,
		unsigned int mdLength,
		std::string & hexEncoded ) {
	AWSv4Impl::convertMessageDigestToLowercaseHex( messageDigest,
		mdLength, hexEncoded );
}


bool doSha256(	const std::string & payload,
				unsigned char * messageDigest,
				unsigned int * mdLength ) {
	return AWSv4Impl::doSha256( payload, messageDigest, mdLength );
}

std::string pathEncode( const std::string & original ) {
    return AWSv4Impl::pathEncode( original );
}

#if defined(EXAMPLE)
bool AmazonMetadataQuery::SendRequest( const std::string & uri ) {
	// Don't know what the meta-data server would do with anything else.
	httpVerb = "GET";
	// Spin the throttling engine up appropriately.
	Throttle::now( & signatureTime );
	// Send a "prepared" request (e.g., don't do any AWS security).
	return sendPreparedRequest( "http", uri, "" );
}
#endif /* defined(EXAMPLE) */

bool AmazonRequest::createV4Signature(	const std::string & payload,
										std::string & authorizationValue,
										bool sendContentSHA ) {
	Throttle::now( & signatureTime );
	time_t now; time( & now );
	struct tm brokenDownTime; gmtime_r( & now, & brokenDownTime );
	// dprintf( D_PERF_TRACE, "request #%d (%s): signature\n", requestID, requestCommand.c_str() );

	//
	// Create task 1's inputs.
	//

	// The canonical URI is the absolute path component of the service URL,
	// normalized according to RFC 3986 (removing redundant and relative
	// path components), with each path segment being URI-encoded.
	std::string protocol, host, canonicalURI;
	if(! parseURL( serviceURL, protocol, host, canonicalURI )) {
		this->errorCode = "E_INVALID_SERVICE_URL";
		this->errorMessage = "Failed to parse service URL.";
		// dprintf( D_ALWAYS, "Failed to match regex against service URL '%s'.\n", serviceURL.c_str() );
		return false;
	}
	if( canonicalURI.empty() ) { canonicalURI = "/"; }

	// But that sounds like a lot of work, so until something we do actually
	// requires it, I'll just assume the path is already normalized.
	canonicalURI = pathEncode( canonicalURI );

	// The canonical query string is the alphabetically sorted list of
	// URI-encoded parameter names '=' values, separated by '&'s.  That
	// wouldn't be hard to do, but we don't need to, since we send
	// everything in the POST body, instead.
	std::string canonicalQueryString;

	// This function doesn't (currently) support query parameters,
	// but no current caller attempts to use them.
	assert( (httpVerb != "GET") || query_parameters.size() == 0 );

	// The canonical headers must include the Host header, so add that
	// now if we don't have it.
	if( headers.find( "Host" ) == headers.end() ) {
		headers[ "Host" ] = host;
	}

	// If we're using temporary credentials, we need to add the token
	// header here as well.  We set saKey and keyID here (well before
	// necessary) since we'll get them for free when we get the token.
	std::string keyID;
	std::string saKey;
	std::string token;
	if( ! readShortFile( this->secretKeyFile, saKey ) ) {
		this->errorCode = "E_FILE_IO";
		this->errorMessage = "Unable to read from secretkey file '" + this->secretKeyFile + "'.";
		// dprintf( D_ALWAYS, "Unable to read secretkey file '%s', failing.\n", this->secretKeyFile.c_str() );
		return false;
	}
	trim( saKey );

	if( ! readShortFile( this->accessKeyFile, keyID ) ) {
		this->errorCode = "E_FILE_IO";
		this->errorMessage = "Unable to read from accesskey file '" + this->accessKeyFile + "'.";
		// dprintf( D_ALWAYS, "Unable to read accesskey file '%s', failing.\n", this->accessKeyFile.c_str() );
		return false;
	}
	trim( keyID );

	// S3 complains if x-amz-date isn't signed, so do this early.
	char dt[] = "YYYYMMDDThhmmssZ";
	strftime( dt, sizeof(dt), "%Y%m%dT%H%M%SZ", & brokenDownTime );
	headers[ "X-Amz-Date" ] = dt;

	char d[] = "YYYYMMDD";
	strftime( d, sizeof(d), "%Y%m%d", & brokenDownTime );

	// S3 complains if x-amz-content-sha256 isn't signed, which makes sense,
	// so do this early.

	// The canonical payload hash is the lowercase hexadecimal string of the
	// (SHA256) hash value of the payload.
	unsigned int mdLength = 0;
	unsigned char messageDigest[EVP_MAX_MD_SIZE];
	if(! doSha256( payload, messageDigest, & mdLength )) {
		this->errorCode = "E_INTERNAL";
		this->errorMessage = "Unable to hash payload.";
		// dprintf( D_ALWAYS, "Unable to hash payload, failing.\n" );
		return false;
	}
	std::string payloadHash;
	convertMessageDigestToLowercaseHex( messageDigest, mdLength, payloadHash );
	if( sendContentSHA ) {
		headers[ "x-amz-content-sha256" ] = payloadHash;
	}

	// The canonical list of headers is a sorted list of lowercase header
	// names paired via ':' with the trimmed header value, each pair
	// terminated with a newline.
	AmazonRequest::AttributeValueMap transformedHeaders;
	for( auto i = headers.begin(); i != headers.end(); ++i ) {
		std::string header = i->first;
		std::transform( header.begin(), header.end(), header.begin(), & tolower );

		std::string value = i->second;
		// We need to leave empty headers alone so that they can be used
		// to disable CURL stupidity later.
		if( value.size() == 0 ) {
			continue;
		}

		// Eliminate trailing spaces.
		unsigned j = value.length() - 1;
		while( value[j] == ' ' ) { --j; }
		if( j != value.length() - 1 ) { value.erase( j + 1 ); }

		// Eliminate leading spaces.
		for( j = 0; value[j] == ' '; ++j ) { }
		value.erase( 0, j );

		// Convert internal runs of spaces into single spaces.
		unsigned left = 1;
		unsigned right = 1;
		bool inSpaces = false;
		while( right < value.length() ) {
			if(! inSpaces) {
				if( value[right] == ' ' ) {
					inSpaces = true;
					left = right;
					++right;
				} else {
					++right;
				}
			} else {
				if( value[right] == ' ' ) {
					++right;
				} else {
					inSpaces = false;
					value.erase( left, right - left - 1 );
					right = left + 1;
				}
			}
		}

		transformedHeaders[ header ] = value;
	}

	// The canonical list of signed headers is trivial to generate while
	// generating the list of headers.
	std::string signedHeaders;
	std::string canonicalHeaders;
	for( auto i = transformedHeaders.begin(); i != transformedHeaders.end(); ++i ) {
		canonicalHeaders += i->first + ":" + i->second + "\n";
		signedHeaders += i->first + ";";
	}
	signedHeaders.erase( signedHeaders.end() - 1 );
	// dprintf( D_ALWAYS, "signedHeaders: '%s'\n", signedHeaders.c_str() );
	// dprintf( D_ALWAYS, "canonicalHeaders: '%s'.\n", canonicalHeaders.c_str() );

	// Task 1: create the canonical request.
	std::string canonicalRequest = httpVerb + "\n"
								 + canonicalURI + "\n"
								 + canonicalQueryString + "\n"
								 + canonicalHeaders + "\n"
								 + signedHeaders + "\n"
								 + payloadHash;
	// fprintf( stderr, "D_SECURITY | D_VERBOSE: canonicalRequest:\n%s\n", canonicalRequest.c_str() );


	//
	// Create task 2's inputs.
	//

	// Hash the canonical request the way we did the payload.
	if(! doSha256( canonicalRequest, messageDigest, & mdLength )) {
		this->errorCode = "E_INTERNAL";
		this->errorMessage = "Unable to hash canonical request.";
		// dprintf( D_ALWAYS, "Unable to hash canonical request, failing.\n" );
		return false;
	}
	std::string canonicalRequestHash;
	convertMessageDigestToLowercaseHex( messageDigest, mdLength, canonicalRequestHash );

	std::string s = this->service;
	if( s.empty() ) {
		size_t i = host.find( "." );
		if( i != std::string::npos ) {
			s = host.substr( 0, i );
		} else {
			// dprintf( D_ALWAYS, "Could not derive service from host '%s'; using host name as service name for testing purposes.\n", host.c_str() );
			s = host;
		}
	}

	std::string r = this->region;
	if( r.empty() ) {
		size_t i = host.find( "." );
		size_t j = host.find( ".", i + 1 );
		if( j != std::string::npos ) {
			r = host.substr( i + 1, j - i - 1 );
		} else {
			// dprintf( D_ALWAYS, "Could not derive region from host '%s'; using host name as region name for testing purposes.\n", host.c_str() );
			r = host;
		}
	}


	// Task 2: create the string to sign.
	std::string credentialScope;
	formatstr( credentialScope, "%s/%s/%s/aws4_request", d, r.c_str(), s.c_str() );
	std::string stringToSign;
	formatstr( stringToSign, "AWS4-HMAC-SHA256\n%s\n%s\n%s",
		dt, credentialScope.c_str(), canonicalRequestHash.c_str() );
	// fprintf( stderr, "D_SECURITY | D_VERBOSE: string to sign:\n%s\n", stringToSign.c_str() );


	//
	// Creating task 3's inputs was done when we checked to see if we needed
	// to get the security token, since they come along for free when we do.
	//

	// Task 3: calculate the signature.
	saKey = "AWS4" + saKey;
	const unsigned char * hmac = HMAC( EVP_sha256(), saKey.c_str(), saKey.length(),
		(unsigned char *)d, sizeof(d) - 1,
		messageDigest, & mdLength );
	if( hmac == NULL ) { return false; }

	unsigned int md2Length = 0;
	unsigned char messageDigest2[EVP_MAX_MD_SIZE];
	hmac = HMAC( EVP_sha256(), messageDigest, mdLength,
		(const unsigned char *)r.c_str(), r.length(), messageDigest2, & md2Length );
	if( hmac == NULL ) { return false; }

	hmac = HMAC( EVP_sha256(), messageDigest2, md2Length,
		(const unsigned char *)s.c_str(), s.length(), messageDigest, & mdLength );
	if( hmac == NULL ) { return false; }

	const char c[] = "aws4_request";
	hmac = HMAC( EVP_sha256(), messageDigest, mdLength,
		(const unsigned char *)c, sizeof(c) - 1, messageDigest2, & md2Length );
	if( hmac == NULL ) { return false; }

	hmac = HMAC( EVP_sha256(), messageDigest2, md2Length,
		(const unsigned char *)stringToSign.c_str(), stringToSign.length(),
		messageDigest, & mdLength );
	if( hmac == NULL ) { return false; }

	std::string signature;
	convertMessageDigestToLowercaseHex( messageDigest, mdLength, signature );

	formatstr( authorizationValue, "AWS4-HMAC-SHA256 Credential=%s/%s,"
				" SignedHeaders=%s, Signature=%s",
				keyID.c_str(), credentialScope.c_str(),
				signedHeaders.c_str(), signature.c_str() );
	// dprintf( D_ALWAYS, "authorization value: '%s'\n", authorizationValue.c_str() );
	return true;
}

bool AmazonRequest::sendV4Request( const std::string & payload, bool sendContentSHA ) {
    std::string protocol, host, path;
    if(! parseURL( serviceURL, protocol, host, path )) {
        this->errorCode = "E_INVALID_SERVICE_URL";
        this->errorMessage = "Failed to parse service URL.";
        // dprintf( D_ALWAYS, "Failed to match regex against service URL '%s'.\n", serviceURL.c_str() );
        return false;
    }
    if( (protocol != "http") && (protocol != "https") ) {
        this->errorCode = "E_INVALID_SERVICE_URL";
        this->errorMessage = "Service URL not of a known protocol (http[s]).";
        // dprintf( D_ALWAYS, "Service URL '%s' not of a known protocol (http[s]).\n", serviceURL.c_str() );
        return false;
    }

    // dprintf( D_FULLDEBUG, "Request URI is '%s'\n", serviceURL.c_str() );
    if(! sendContentSHA) {
    	// dprintf( D_FULLDEBUG, "Payload is '%s'\n", payload.c_str() );
    }

    std::string authorizationValue;
    if(! createV4Signature( payload, authorizationValue, sendContentSHA )) {
        if( this->errorCode.empty() ) { this->errorCode = "E_INTERNAL"; }
        if( this->errorMessage.empty() ) { this->errorMessage = "Failed to create v4 signature."; }
        // dprintf( D_ALWAYS, "Failed to create v4 signature.\n" );
        return false;
    }
    headers[ "Authorization" ] = authorizationValue;

    return sendPreparedRequest( protocol, serviceURL, payload );
}

#if defined(SUPPORT_V2)
bool AmazonRequest::sendV2Request() {
    //
    // Every request must have the following parameters:
    //
    //      Action, Version, AWSAccessKeyId, Timestamp (or Expires),
    //      Signature, SignatureMethod, and SignatureVersion.
    //

    if( query_parameters.find( "Action" ) == query_parameters.end() ) {
        this->errorCode = "E_INTERNAL";
        this->errorMessage = "No action specified in request.";
        // dprintf( D_ALWAYS, "No action specified in request, failing.\n" );
        return false;
    }

    // We need to know right away if we're doing FermiLab-style authentication.
    //
    // While we're at it, extract "the value of the Host header in lowercase"
    // and the "HTTP Request URI" from the service URL.  The service URL must
    // be of the form '[http[s]|x509|euca3[s]]://hostname[:port][/path]*'.
    std::string protocol, host, httpRequestURI;
    if(! parseURL( serviceURL, protocol, host, httpRequestURI )) {
        this->errorCode = "E_INVALID_SERVICE_URL";
        this->errorMessage = "Failed to parse service URL.";
        // dprintf( D_ALWAYS, "Failed to match regex against service URL '%s'.\n", serviceURL.c_str() );
        return false;
    }

    if( (protocol != "http" && protocol != "https" && protocol != "x509" && protocol != "euca3" && protocol != "euca3s" ) ) {
        this->errorCode = "E_INVALID_SERVICE_URL";
        this->errorMessage = "Service URL not of a known protocol (http[s]|x509|euca3[s]).";
        // dprintf( D_ALWAYS, "Service URL '%s' not of a known protocol (http[s]|x509|euca3[s]).\n", serviceURL.c_str() );
        return false;
    }
    std::string hostAndPath = host + httpRequestURI;
    std::transform( host.begin(), host.end(), host.begin(), & tolower );
    if( httpRequestURI.empty() ) { httpRequestURI = "/"; }

    //
    // Eucalyptus 3 bombs if it sees this attribute.
    //
    if( protocol == "euca3" || protocol == "euca3s" ) {
        query_parameters.erase( "InstanceInitiatedShutdownBehavior" );
    }

    //
    // The AWSAccessKeyId is just the contents of this->accessKeyFile,
    // and are (currently) 20 characters long.
    //
    std::string keyID;
    if( protocol != "x509" ) {
        if( ! readShortFile( this->accessKeyFile, keyID ) ) {
            this->errorCode = "E_FILE_IO";
            this->errorMessage = "Unable to read from accesskey file '" + this->accessKeyFile + "'.";
            // dprintf( D_ALWAYS, "Unable to read accesskey file '%s', failing.\n", this->accessKeyFile.c_str() );
            return false;
        }
        trim( keyID );
        query_parameters.insert( std::make_pair( "AWSAccessKeyId", keyID ) );
    }

    //
    // This implementation computes signature version 2,
    // using the "HmacSHA256" method.
    //
    query_parameters.insert( std::make_pair( "SignatureVersion", "2" ) );
    query_parameters.insert( std::make_pair( "SignatureMethod", "HmacSHA256" ) );

    //
    // We're calculating the signature now. [YYYY-MM-DDThh:mm:ssZ]
    //
    Throttle::now( & signatureTime );
    time_t now; time( & now );
    struct tm brokenDownTime; gmtime_r( & now, & brokenDownTime );
    char iso8601[] = "YYYY-MM-DDThh:mm:ssZ";
    strftime( iso8601, 20, "%Y-%m-%dT%H:%M:%SZ", & brokenDownTime );
    query_parameters.insert( std::make_pair( "Timestamp", iso8601 ) );
    // dprintf( D_PERF_TRACE, "request #%d (%s): signature\n", requestID, requestCommand.c_str() );


    /*
     * The tricky party of sending a Query API request is calculating
     * the signature.  See
     *
     * http://docs.amazonwebservices.com/AWSEC2/2010-11-15/DeveloperGuide/using-query-api.html
     *
     */

    // Step 1: Create the canonicalized query string.
    std::string canonicalQueryString = canonicalizeQueryString();

    // Step 2: Create the string to sign.
    std::string stringToSign = "POST\n"
                             + host + "\n"
                             + httpRequestURI + "\n"
                             + canonicalQueryString;

    // Step 3: "Calculate an RFC 2104-compliant HMAC with the string
    // you just created, your Secret Access Key as the key, and SHA256
    // or SHA1 as the hash algorithm."
    std::string saKey;
    if( protocol == "x509" ) {
        // If we ever support the UploadImage action, we'll need to
        // extract the DN from the user's certificate here.  Otherwise,
        // since the x.509 implementation ignores the AWSAccessKeyId
        // and Signature, we can do whatever we want.
        saKey = std::string( "not-the-DN" );
        // dprintf( D_FULLDEBUG, "Using '%s' as secret key for x.509\n", saKey.c_str() );
    } else {
        if( ! readShortFile( this->secretKeyFile, saKey ) ) {
            this->errorCode = "E_FILE_IO";
            this->errorMessage = "Unable to read from secretkey file '" + this->secretKeyFile + "'.";
            // dprintf( D_ALWAYS, "Unable to read secretkey file '%s', failing.\n", this->secretKeyFile.c_str() );
            return false;
        }
        trim( saKey );
    }

    unsigned int mdLength = 0;
    unsigned char messageDigest[EVP_MAX_MD_SIZE];
    const unsigned char * hmac = HMAC( EVP_sha256(), saKey.c_str(), saKey.length(),
        (const unsigned char *)stringToSign.c_str(), stringToSign.length(), messageDigest, & mdLength );
    if( hmac == NULL ) {
        this->errorCode = "E_INTERNAL";
        this->errorMessage = "Unable to calculate query signature (SHA256 HMAC).";
        // dprintf( D_ALWAYS, "Unable to calculate SHA256 HMAC to sign query, failing.\n" );
        return false;
    }

    // Step 4: "Convert the resulting value to base64."
    char * base64Encoded = condor_base64_encode( messageDigest, mdLength );
    std::string signatureInBase64 = base64Encoded;
    free( base64Encoded );

    // Generate the final URI.
    canonicalQueryString += "&Signature=" + amazonURLEncode( signatureInBase64 );
    std::string postURI;
    if( protocol == "x509" ) {
        postURI = "https://" + hostAndPath;
    } else if( protocol == "euca3" ) {
        postURI = "http://" + hostAndPath;
    } else if( protocol == "euca3s" ) {
        postURI = "https://" + hostAndPath;
    } else {
        postURI = this->serviceURL;
    }
    // dprintf( D_FULLDEBUG, "Request URI is '%s'\n", postURI.c_str() );

    // The AWS docs now say that " " - > "%20", and that "+" is an error.
    size_t index = canonicalQueryString.find( "AWSAccessKeyId=" );
    if( index != std::string::npos ) {
        size_t skipLast = canonicalQueryString.find( "&", index + 14 );
        char swap = canonicalQueryString[ index + 15 ];
        canonicalQueryString[ index + 15 ] = '\0';
        char const * cqs = canonicalQueryString.c_str();
        if( skipLast == std::string::npos ) {
            // dprintf( D_FULLDEBUG, "Post body is '%s...'\n", cqs );
        } else {
            // dprintf( D_FULLDEBUG, "Post body is '%s...%s'\n", cqs, cqs + skipLast );
        }
        canonicalQueryString[ index + 15 ] = swap;
    } else {
        // dprintf( D_FULLDEBUG, "Post body is '%s'\n", canonicalQueryString.c_str() );
    }

    return sendPreparedRequest( protocol, postURI, canonicalQueryString );
}
#endif /* defined(SUPPORT_V2) */

#if defined(NEEDED)
bool AmazonRequest::SendURIRequest() {
    httpVerb = "GET";
    std::string noPayloadAllowed;
    return sendV4Request( noPayloadAllowed );
}

bool AmazonRequest::SendJSONRequest( const std::string & payload ) {
    headers[ "Content-Type" ] = "application/x-amz-json-1.1";
    return sendV4Request( payload );
}
#endif /* defined(NEEDED) */

// It's stated in the API documentation that you can upload to any region
// via us-east-1, which is moderately crazy.
bool AmazonRequest::SendS3Request( const std::string & payload ) {
	headers[ "Content-Type" ] = "binary/octet-stream";
	std::string contentLength; formatstr( contentLength, "%zu", payload.size() );
	headers[ "Content-Length" ] = contentLength;
	// Another undocumented CURL feature: transfer-encoding is "chunked"
	// by default for "PUT", which we really don't want.
	headers[ "Transfer-Encoding" ] = "";
	service = "s3";
	if( region.empty() ) {
		region = "us-east-1";
	}
	return sendV4Request( payload, true );
}

int
debug_callback( CURL *, curl_infotype ci, char * data, size_t size, void * ) {
	switch( ci ) {
		default:
			// dprintf( D_ALWAYS, "debug_callback( unknown )\n" );
			break;

		case CURLINFO_TEXT:
			// dprintf( D_ALWAYS, "debug_callback( TEXT ): '%*s'\n", (int)size, data );
			break;

		case CURLINFO_HEADER_IN:
			// dprintf( D_ALWAYS, "debug_callback( HEADER_IN ): '%*s'\n", (int)size, data );
			break;

		case CURLINFO_HEADER_OUT:
			// dprintf( D_ALWAYS, "debug_callback( HEADER_IN ): '%*s'\n", (int)size, data );
			break;

		case CURLINFO_DATA_IN:
			// dprintf( D_ALWAYS, "debug_callback( DATA_IN )\n" );
			break;

		case CURLINFO_DATA_OUT:
			// dprintf( D_ALWAYS, "debug_callback( DATA_OUT )\n" );
			break;

		case CURLINFO_SSL_DATA_IN:
			// dprintf( D_ALWAYS, "debug_callback( SSL_DATA_IN )\n" );
			break;

		case CURLINFO_SSL_DATA_OUT:
			// dprintf( D_ALWAYS, "debug_callback( SSL_DATA_OUT )\n" );
			break;
	}

	return 0;
}

size_t
read_callback( char * buffer, size_t size, size_t n, void * v ) {
	// This can be static because only one curl_easy_perform() can be
	// running at a time.
	static size_t sentSoFar = 0;
	std::string * payload = (std::string *)v;

	if( sentSoFar == payload->size() ) {
		// dprintf( D_ALWAYS, "read_callback(): resetting sentSoFar.\n" );
		sentSoFar = 0;
		return 0;
	}

	size_t request = size * n;
	if( request > payload->size() ) { request = payload->size(); }

	if( sentSoFar + request > payload->size() ) {
		request = payload->size() - sentSoFar;
	}

	// dprintf( D_ALWAYS, "read_callback(): sending %lu (sent %lu already).\n", request, sentSoFar );
	memcpy( buffer, payload->data() + sentSoFar, request );
	sentSoFar += request;

	return request;
}

bool AmazonRequest::sendPreparedRequest(
        const std::string & protocol,
        const std::string & uri,
        const std::string & payload ) {
    static bool rateLimitInitialized = false;
    if(! rateLimitInitialized) {
        globalCurlThrottle.rateLimit = 100;
        // dprintf( D_PERF_TRACE, "rate limit = %d\n", globalCurlThrottle.rateLimit );
        rateLimitInitialized = true;
    }

    // curl_global_init() is not thread-safe.  However, it's safe to call
    // multiple times.  Therefore, we'll just call it before we drop the
    // mutex, since we know that means only one thread is running.
    CURLcode rv = curl_global_init( CURL_GLOBAL_ALL );
    if( rv != 0 ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_global_init() failed.";
        // dprintf( D_ALWAYS, "curl_global_init() failed, failing.\n" );
        return false;
    }

    std::unique_ptr<CURL,decltype(&curl_easy_cleanup)> curl(curl_easy_init(), &curl_easy_cleanup);

    if( curl.get() == NULL ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_init() failed.";
        // dprintf( D_ALWAYS, "curl_easy_init() failed, failing.\n" );
        return false;
    }

    char errorBuffer[CURL_ERROR_SIZE];
    rv = curl_easy_setopt( curl.get(), CURLOPT_ERRORBUFFER, errorBuffer );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_ERRORBUFFER ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_ERRORBUFFER ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }


/*
    rv = curl_easy_setopt( curl.get(), CURLOPT_DEBUGFUNCTION, debug_callback );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_DEBUGFUNCTION ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_DEBUGFUNCTION ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }

    // CURLOPT_DEBUGFUNCTION does nothing without CURLOPT_DEBUG set.
    rv = curl_easy_setopt( curl.get(), CURLOPT_VERBOSE, 1 );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_VERBOSE ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_VERBOSE ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }
*/


    // dprintf( D_ALWAYS, "sendPreparedRequest(): CURLOPT_URL = '%s'\n", uri.c_str() );
    rv = curl_easy_setopt( curl.get(), CURLOPT_URL, uri.c_str() );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_URL ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_URL ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }

    if( httpVerb == "HEAD" ) {
        rv = curl_easy_setopt( curl.get(), CURLOPT_NOBODY, 1 );

		if( rv != CURLE_OK ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_HEAD ) failed.";
			// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_HEAD ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
			return false;
		}
    }

	if( httpVerb == "POST" ) {
		rv = curl_easy_setopt( curl.get(), CURLOPT_POST, 1 );
		if( rv != CURLE_OK ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_POST ) failed.";
			// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_POST ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
			return false;
		}

		// dprintf( D_ALWAYS, "sendPreparedRequest(): CURLOPT_POSTFIELDS = '%s'\n", payload.c_str() );
		rv = curl_easy_setopt( curl.get(), CURLOPT_POSTFIELDS, payload.c_str() );
		if( rv != CURLE_OK ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_POSTFIELDS ) failed.";
			// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_POSTFIELDS ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
			return false;
		}
	}

	if( httpVerb == "PUT" ) {
		rv = curl_easy_setopt( curl.get(), CURLOPT_UPLOAD, 1 );
		if( rv != CURLE_OK ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_UPLOAD ) failed.";
			// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_UPLOAD ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
			return false;
		}

		rv = curl_easy_setopt( curl.get(), CURLOPT_READDATA, & payload );
		if( rv != CURLE_OK ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_READDATA ) failed.";
			// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_READDATA ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
			return false;
		}

		rv = curl_easy_setopt( curl.get(), CURLOPT_READFUNCTION, read_callback );
		if( rv != CURLE_OK ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_easy_setopt( CURLOPT_READFUNCTION ) failed.";
			// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_READFUNCTION ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
			return false;
		}
	}

    rv = curl_easy_setopt( curl.get(), CURLOPT_NOPROGRESS, 1 );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_NOPROGRESS ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_NOPROGRESS ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }

    if ( includeResponseHeader ) {
        rv = curl_easy_setopt( curl.get(), CURLOPT_HEADER, 1 );
        if( rv != CURLE_OK ) {
            this->errorCode = "E_CURL_LIB";
            this->errorMessage = "curl_easy_setopt( CURLOPT_HEADER ) failed.";
            // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_HEADER ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
            return false;
        }
    }

    rv = curl_easy_setopt( curl.get(), CURLOPT_WRITEFUNCTION, & appendToString );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_WRITEFUNCTION ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_WRITEFUNCTION ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }

    rv = curl_easy_setopt( curl.get(), CURLOPT_WRITEDATA, & this->resultString );
    if( rv != CURLE_OK ) {
        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_setopt( CURLOPT_WRITEDATA ) failed.";
        // dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_WRITEDATA ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        return false;
    }

    //
    // Set security options.
    //
    SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSL_VERIFYPEER, 1 );
    SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSL_VERIFYHOST, 2 );

    // NB: Contrary to libcurl's manual, it doesn't strdup() strings passed
    // to it, so they MUST remain in scope until after we call
    // curl_easy_cleanup().  Otherwise, curl_perform() will fail with
    // a completely bogus error, number 60, claiming that there's a
    // 'problem with the SSL CA cert'.
    std::string CAFile = "";
    std::string CAPath = "";

    char * x509_ca_dir = getenv( "X509_CERT_DIR" );
    if( x509_ca_dir != NULL ) {
        CAPath = x509_ca_dir;
    }

    char * x509_ca_file = getenv( "X509_CERT_FILE" );
    if( x509_ca_file != NULL ) {
        CAFile = x509_ca_file;
    }

    if( CAPath.empty() ) {
        char * soap_ssl_ca_dir = getenv( "GAHP_SSL_CADIR" );
        if( soap_ssl_ca_dir != NULL ) {
            CAPath = soap_ssl_ca_dir;
        }
    }

    if( CAFile.empty() ) {
        char * soap_ssl_ca_file = getenv( "GAHP_SSL_CAFILE" );
        if( soap_ssl_ca_file != NULL ) {
            CAFile = soap_ssl_ca_file;
        }
    }

    if( ! CAPath.empty() ) {
        // dprintf( D_FULLDEBUG, "Setting CA path to '%s'\n", CAPath.c_str() );
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_CAPATH, CAPath.c_str() );
    }

    if( ! CAFile.empty() ) {
        // dprintf( D_FULLDEBUG, "Setting CA file to '%s'\n", CAFile.c_str() );
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_CAINFO, CAFile.c_str() );
    }

    if( setenv( "OPENSSL_ALLOW_PROXY", "1", 0 ) != 0 ) {
        // dprintf( D_FULLDEBUG, "Failed to set OPENSSL_ALLOW_PROXY.\n" );
    }

    //
    // Configure for x.509 operation.
    //
    if( protocol == "x509" ) {
        // dprintf( D_FULLDEBUG, "Configuring x.509...\n" );

        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLKEYTYPE, "PEM" );
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLKEY, this->secretKeyFile.c_str() );

        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLCERTTYPE, "PEM" );
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLCERT, this->accessKeyFile.c_str() );
    }


	std::string headerPair;
	struct curl_slist * header_slist = NULL;
	for( auto i = headers.begin(); i != headers.end(); ++i ) {
		formatstr( headerPair, "%s: %s", i->first.c_str(), i->second.c_str() );
		// dprintf( D_FULLDEBUG, "sendPreparedRequest(): adding header = '%s: %s'\n", i->first.c_str(), i->second.c_str() );
		header_slist = curl_slist_append( header_slist, headerPair.c_str() );
		if( header_slist == NULL ) {
			this->errorCode = "E_CURL_LIB";
			this->errorMessage = "curl_slist_append() failed.";
			// dprintf( D_ALWAYS, "curl_slist_append() failed, failing.\n" );
			return false;
		}
	}

	rv = curl_easy_setopt( curl.get(), CURLOPT_HTTPHEADER, header_slist );
	if( rv != CURLE_OK ) {
		this->errorCode = "E_CURL_LIB";
		this->errorMessage = "curl_easy_setopt( CURLOPT_HTTPHEADER ) failed.";
		// dprintf( D_ALWAYS, "curl_easy_setopt( CURLOPT_HTTPHEADER ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
		if( header_slist ) { curl_slist_free_all( header_slist ); }
		return false;
	}


    // static unsigned failureCount = 0;
    // dprintf( D_PERF_TRACE, "request #%d (%s): ready to call %s\n", requestID, requestCommand.c_str(), query_parameters[ "Action" ].c_str() );

    // Throttle::now( & this->mutexReleased );
    // amazon_gahp_release_big_mutex();
    pthread_mutex_lock( & globalCurlMutex );
    // Throttle::now( & this->lockGained );

    // We don't check the deadline after the retry because limitExceeded()
    // already checks.  (limitNotExceeded() does not, but if we call that
    // then the request has succeeded and we won't be retrying.)
    globalCurlThrottle.setDeadline( signatureTime, 300 );
    struct timespec liveline = globalCurlThrottle.getWhen();
    struct timespec deadline = globalCurlThrottle.getDeadline();
    if( Throttle::difference( & liveline, & deadline ) < 0 ) {
        // amazon_gahp_grab_big_mutex();
        // dprintf( D_PERF_TRACE, "request #%d (%s): deadline would be exceeded\n", requestID, requestCommand.c_str() );
        // failureCount = 0;

        this->errorCode = "E_DEADLINE_WOULD_BE_EXCEEDED";
        this->errorMessage = "Signature would have expired before next permissible time to use it.";
        if( header_slist ) { curl_slist_free_all( header_slist ); }

        pthread_mutex_unlock( & globalCurlMutex );
        return false;
    }

retry:
    // this->liveLine = globalCurlThrottle.getWhen();
    // Throttle::now( & this->sleepBegan );
    globalCurlThrottle.sleepIfNecessary();
    // Throttle::now( & this->sleepEnded );

    // Throttle::now( & this->requestBegan );
    rv = curl_easy_perform( curl.get() );
    // Throttle::now( & this->requestEnded );

    // amazon_gahp_grab_big_mutex();
    // Throttle::now( & this->mutexGained );

    // dprintf( D_PERF_TRACE, "request #%d (%s): called %s\n", requestID, requestCommand.c_str(), query_parameters[ "Action" ].c_str() );
    /*
    dprintf( D_PERF_TRACE | D_VERBOSE,
        "request #%d (%s): "
        "scheduling delay (release mutex, grab lock): %ld ms; "
        "cumulative delay time: %ld ms; "
        "last delay time: %ld ms "
        "request time: %ld ms; "
        "grab mutex: %ld ms\n",
        requestID, requestCommand.c_str(),
        Throttle::difference( & this->mutexReleased, & this->lockGained ),
        Throttle::difference( & this->lockGained, & this->requestBegan ),
        Throttle::difference( & this->sleepBegan, & this->sleepEnded ),
        Throttle::difference( & this->requestBegan, & this->requestEnded ),
        Throttle::difference( & this->requestEnded, & this->mutexGained )
    );
    */

    if( rv != 0 ) {
        // We'll be very conservative here, and set the next liveline as if
        // this request had exceeded the server's rate limit, which will also
        // set the client-side rate limit if that's larger.  Since we're
        // already terminally failing the request, don't bother to check if
        // this was our last chance at retrying.
        globalCurlThrottle.limitExceeded();

        this->errorCode = "E_CURL_IO";
        std::ostringstream error;
        error << "curl_easy_perform() failed (" << rv << "): '" << curl_easy_strerror( rv ) << "'.";
        this->errorMessage = error.str();
        // dprintf( D_ALWAYS, "%s\n", this->errorMessage.c_str() );
        // dprintf( D_FULLDEBUG, "%s\n", errorBuffer );
        if( header_slist ) { curl_slist_free_all( header_slist ); }

        pthread_mutex_unlock( & globalCurlMutex );
        return false;
    }

    responseCode = 0;
    rv = curl_easy_getinfo( curl.get(), CURLINFO_RESPONSE_CODE, & responseCode );
    if( rv != CURLE_OK ) {
        // So we contacted the server but it returned such gibberish that
        // CURL couldn't identify the response code.  Let's assume that's
        // bad news.  Since we're already terminally failing the request,
        // don't bother to check if this was our last chance at retrying.
        globalCurlThrottle.limitExceeded();

        this->errorCode = "E_CURL_LIB";
        this->errorMessage = "curl_easy_getinfo() failed.";
        // dprintf( D_ALWAYS, "curl_easy_getinfo( CURLINFO_RESPONSE_CODE ) failed (%d): '%s', failing.\n", rv, curl_easy_strerror( rv ) );
        if( header_slist ) { curl_slist_free_all( header_slist ); }

        pthread_mutex_unlock( & globalCurlMutex );
        return false;
    }

    if( responseCode == 503 && (resultString.find( "<Error><Code>RequestLimitExceeded</Code>" ) != std::string::npos) ) {
        if( globalCurlThrottle.limitExceeded() ) {
            // dprintf( D_PERF_TRACE, "request #%d (%s): retry limit exceeded\n", requestID, requestCommand.c_str() );
        	// failureCount = 0;

            // This should almost certainly be E_REQUEST_LIMIT_EXCEEDED, but
            // for now return the same error code for this condition that
            // we did before we recongized it.
            formatstr( this->errorCode, "E_HTTP_RESPONSE_NOT_200 (%lu)", responseCode );
            this->errorMessage = resultString;
	        if( header_slist ) { curl_slist_free_all( header_slist ); }

            pthread_mutex_unlock( & globalCurlMutex );
            return false;
        }

        // dprintf( D_PERF_TRACE, "request #%d (%s): will retry\n", requestID, requestCommand.c_str() );
        resultString.clear();
        // amazon_gahp_release_big_mutex();
        goto retry;
    } else {
        globalCurlThrottle.limitNotExceeded();
    }

    if( header_slist ) { curl_slist_free_all( header_slist ); }

    if( responseCode != this->expectedResponseCode ) {
        formatstr( this->errorCode, "E_HTTP_RESPONSE_NOT_EXPECTED (response %lu != expected %lu)", responseCode, this->expectedResponseCode );
        this->errorMessage = resultString;
        if( this->errorMessage.empty() ) {
            formatstr( this->errorMessage, "HTTP response was %lu, not %lu, and no body was returned.", responseCode, this->expectedResponseCode );
        }
        // fprintf( stderr, "D_FULLDEBUG: Query did not return 200 (%lu), failing.\n", responseCode );
        // fprintf( stderr, "D_FULLDEBUG: Failure response text was '%s'.\n", resultString.c_str() );

        // dprintf( D_PERF_TRACE, "request #%d (%s): call to %s returned %lu.\n", requestID, requestCommand.c_str(), query_parameters[ "Action" ].c_str(), responseCode );
        pthread_mutex_unlock( & globalCurlMutex );
        return false;
    }

    // dprintf( D_FULLDEBUG, "Response was '%s'\n", resultString.c_str() );
    pthread_mutex_unlock( & globalCurlMutex );
    // dprintf( D_PERF_TRACE, "request #%d (%s): call to %s returned 200 (OK).\n", requestID, requestCommand.c_str(), query_parameters[ "Action" ].c_str() );
    return true;
}

// ---------------------------------------------------------------------------

AmazonS3Upload::~AmazonS3Upload() { }

bool AmazonS3Upload::SendRequest( const std::string & payload, off_t offset, size_t size ) {
	std::string protocol, host, canonicalURI;
	if(! parseURL( serviceURL, protocol, host, canonicalURI )) {
		errorCode = "E_INVALID_SERVICE_URL";
		errorMessage = "Failed to parse service URL.";
		// dprintf( D_ALWAYS, "Failed to match regex against service URL '%s'.\n", serviceURL.c_str() );

		return false;
	}
	if( canonicalURI.empty() ) { canonicalURI = "/"; }

	serviceURL = protocol + "://" + bucket + "." +
				 host + canonicalURI + object;

	// If we can, set the region based on the host.
	size_t secondDot = host.find( ".", 2 + 1 );
	if( host.find( "s3." ) == 0 ) {
	    region = host.substr( 3, secondDot - 2 - 1 );
	}

	if( offset != 0 || size != 0 ) {
		std::string range;
		formatstr( range, "bytes=%zu-%zu", offset, offset + size );
		headers["Range"] = range.c_str();
	}

	httpVerb = "PUT";
	return SendS3Request( payload );
}

// ---------------------------------------------------------------------------

AmazonS3Download::~AmazonS3Download() { }

bool AmazonS3Download::SendRequest( off_t offset, size_t size ) {
	std::string protocol, host, canonicalURI;
	if(! parseURL( serviceURL, protocol, host, canonicalURI )) {
		errorCode = "E_INVALID_SERVICE_URL";
		errorMessage = "Failed to parse service URL.";
		// dprintf( D_ALWAYS, "Failed to match regex against service URL '%s'.\n", serviceURL.c_str() );

		return false;
	}
	if( canonicalURI.empty() ) { canonicalURI = "/"; }

	serviceURL = protocol + "://" + bucket + "." +
				 host + canonicalURI + object;

	// If we can, set the region based on the host.
	size_t secondDot = host.find( ".", 2 + 1 );
	if( host.find( "s3." ) == 0 ) {
	    region = host.substr( 3, secondDot - 2 - 1 );
	}


	if( offset != 0 || size != 0 ) {
		std::string range;
		formatstr( range, "bytes=%zu-%zu", offset, offset + size );
		headers["Range"] = range.c_str();
		this->expectedResponseCode = 206;
	}

	httpVerb = "GET";
	std::string noPayloadAllowed;
	return SendS3Request( noPayloadAllowed );
}

// ---------------------------------------------------------------------------

AmazonS3Head::~AmazonS3Head() { }

bool AmazonS3Head::SendRequest() {
	std::string protocol, host, canonicalURI;
	if(! parseURL( serviceURL, protocol, host, canonicalURI )) {
		errorCode = "E_INVALID_SERVICE_URL";
		errorMessage = "Failed to parse service URL.";
		// dprintf( D_ALWAYS, "Failed to match regex against service URL '%s'.\n", serviceURL.c_str() );

		return false;
	}
	if( canonicalURI.empty() ) { canonicalURI = "/"; }

	serviceURL = protocol + "://" + bucket + "." +
				 host + canonicalURI + object;

	// If we can, set the region based on the host.
	size_t secondDot = host.find( ".", 2 + 1 );
	if( host.find( "s3." ) == 0 ) {
	    region = host.substr( 3, secondDot - 2 - 1 );
	}


	// service = "s3";
	httpVerb = "HEAD";
	includeResponseHeader = true;
	std::string noPayloadAllowed;
	return SendS3Request( noPayloadAllowed );
}

// ---------------------------------------------------------------------------
