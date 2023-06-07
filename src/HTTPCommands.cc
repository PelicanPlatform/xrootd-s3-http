#include <sstream>
#include <algorithm>
#include <openssl/hmac.h>
#include <curl/curl.h>
#include <cassert>
#include <cstring>
#include <memory>
#include <filesystem>

#include <map>
#include <string>

#include "HTTPCommands.hh"
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

HTTPRequest::~HTTPRequest() { }

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

bool HTTPRequest::parseProtocol(
        const std::string & url,
        std::string & protocol ) {

    auto i = url.find( "://" );
    if( i == std::string::npos ) { return false; }
    protocol = substring( url, 0, i );

    // This func used to parse the entire URL according
    // to the Amazon canonicalURI specs, but that functionality
    // has since been moved to the Amazon subclass. Now it just
    // grabs the protocol. Leaving the old stuff commented for
    // now, just in case...

    // auto j = url.find( "/", i + 3 );
    // if( j == std::string::npos ) {
    //     host = substring( url, i + 3 );
    //     path = "/";
    //     return true;
    // }

    // host = substring( url, i + 3, j );
    // path = substring( url, j );
    return true;
}

bool HTTPRequest::SendHTTPRequest( const std::string & payload ) {
    if( (protocol != "http") && (protocol != "https") ) {
        this->errorCode = "E_INVALID_SERVICE_URL";
        this->errorMessage = "Service URL not of a known protocol (http[s]).";
        // dprintf( D_ALWAYS, "Service URL '%s' not of a known protocol (http[s]).\n", serviceURL.c_str() );
        return false;
    }

    headers[ "Content-Type" ] = "binary/octet-stream";
	std::string contentLength; formatstr( contentLength, "%zu", payload.size() );
	headers[ "Content-Length" ] = contentLength;
	// Another undocumented CURL feature: transfer-encoding is "chunked"
	// by default for "PUT", which we really don't want.
	headers[ "Transfer-Encoding" ] = "";


	return sendPreparedRequest( protocol, hostUrl, payload );
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

bool HTTPRequest::sendPreparedRequest(
        const std::string & protocol,
        const std::string & uri,
        const std::string & payload ) {

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

    // TODO: Get this section to work with HTTP stuff without being so hacky
    if( protocol == "x509" ) {
        // dprintf( D_FULLDEBUG, "Configuring x.509...\n" );

        if (requiresSignature) // If requiresSignature is true, there will be
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLKEYTYPE, "PEM" );
        //SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLKEY, this->secretKeyFile.c_str() );
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLKEY, "" );

        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLCERTTYPE, "PEM" );
        //SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLCERT, this->accessKeyFile.c_str() );
        SET_CURL_SECURITY_OPTION( curl.get(), CURLOPT_SSLCERT, "" );
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
    
    // Comment the mutex, causing problems and libcurl appears threadsafe anyway
    //pthread_mutex_lock( & globalCurlMutex );
    
    // Throttle::now( & this->lockGained );

    // We don't check the deadline after the retry because limitExceeded()
    // already checks.  (limitNotExceeded() does not, but if we call that
    // then the request has succeeded and we won't be retrying.)

    if (requiresSignature) {
        // XRootD Already has its own throttling plugin, so commenting the throttle-related code for not        
        static bool rateLimitInitialized = false;
        if(! rateLimitInitialized) {
            globalCurlThrottle.rateLimit = 100;
            // dprintf( D_PERF_TRACE, "rate limit = %d\n", globalCurlThrottle.rateLimit );
            rateLimitInitialized = true;
        }

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

        //pthread_mutex_unlock( & globalCurlMutex );
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

        //pthread_mutex_unlock( & globalCurlMutex );
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

            //pthread_mutex_unlock( & globalCurlMutex );
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
        //pthread_mutex_unlock( & globalCurlMutex );
        return false;
    }

    // dprintf( D_FULLDEBUG, "Response was '%s'\n", resultString.c_str() );
    //pthread_mutex_unlock( & globalCurlMutex );
    // dprintf( D_PERF_TRACE, "request #%d (%s): call to %s returned 200 (OK).\n", requestID, requestCommand.c_str(), query_parameters[ "Action" ].c_str() );
    return true;
}

// ---------------------------------------------------------------------------

HTTPUpload::~HTTPUpload() { }

bool HTTPUpload::SendRequest( const std::string & payload, off_t offset, size_t size ) {
	if( offset != 0 || size != 0 ) {
		std::string range;
		formatstr( range, "bytes=%zu-%zu", offset, offset + size - 1 );
		headers["Range"] = range.c_str();
	}

	httpVerb = "PUT";
	return SendHTTPRequest( payload );
}

// ---------------------------------------------------------------------------

HTTPDownload::~HTTPDownload() { }

bool HTTPDownload::SendRequest( off_t offset, size_t size ) {
	if( offset != 0 || size != 0 ) {
		std::string range;
		formatstr( range, "bytes=%zu-%zu", offset, offset + size - 1 );
		headers["Range"] = range.c_str();
		this->expectedResponseCode = 206;
	}

	httpVerb = "GET";
	std::string noPayloadAllowed;
	return SendHTTPRequest( noPayloadAllowed );
}

// ---------------------------------------------------------------------------

HTTPHead::~HTTPHead() { }

bool HTTPHead::SendRequest() {
	httpVerb = "HEAD";
	includeResponseHeader = true;
	std::string noPayloadAllowed;
	return SendHTTPRequest( noPayloadAllowed );
}

// ---------------------------------------------------------------------------
