#ifndef S3_COMMANDS_H
#define S3_COMMANDS_H

// #include <map>
// #include <string>
// #include "s3Commands.hh"

class AmazonRequest {
    public:
        AmazonRequest(
            const std::string & s,
            const std::string & akf,
            const std::string & skf,
            int sv = 4
        ) :
            serviceURL(s),
            accessKeyFile(akf),
            secretKeyFile(skf),
            responseCode(0),
            includeResponseHeader(false),
            signatureVersion(sv),
            httpVerb( "POST" )
        { }
        virtual ~AmazonRequest();

        virtual bool SendRequest();
        // virtual bool SendURIRequest();
        // virtual bool SendJSONRequest( const std::string & payload );
        virtual bool SendS3Request( const std::string & payload );

        unsigned long getResponseCode() const { return responseCode; }
        const std::string & getResultString() const { return resultString; }

    protected:
        bool sendV4Request( const std::string & payload, bool sendContentSHA = false );

        bool sendPreparedRequest(   const std::string & protocol,
                                    const std::string & uri,
                                    const std::string & payload );

        typedef std::map< std::string, std::string > AttributeValueMap;
        AttributeValueMap query_parameters;
        AttributeValueMap headers;

        std::string serviceURL;
        std::string accessKeyFile;
        std::string secretKeyFile;

        std::string errorMessage;
        std::string errorCode;

        std::string resultString;
        unsigned long responseCode;
        bool includeResponseHeader;

        // So that we don't bother to send expired signatures.
        struct timespec signatureTime;

        int signatureVersion;

        std::string region;
        std::string service;
        std::string httpVerb;

    private:
        bool createV4Signature( const std::string & payload, std::string & authorizationHeader, bool sendContentSHA = false );

        std::string canonicalizeQueryString();
};

class AmazonS3Upload : public AmazonRequest {
    public:
        AmazonS3Upload(
            const std::string & s,
            const std::string & akf,
            const std::string & skf,
            const std::string & b,
            const std::string & o,
            const std::string & p
        ) :
            AmazonRequest(s, akf, skf),
            bucket(b),
            object(o),
            path(p)
        { }

        virtual ~AmazonS3Upload();

        virtual bool SendRequest();

    protected:
        std::string bucket;
        std::string object;
        std::string path;
};

class AmazonS3Download : public AmazonRequest {
    public:
        AmazonS3Download(
            const std::string & s,
            const std::string & akf,
            const std::string & skf,
            const std::string & b,
            const std::string & o
        ) :
            AmazonRequest(s, akf, skf),
            bucket(b),
            object(o)
        { }

        virtual ~AmazonS3Download();

        virtual bool SendRequest( off_t offset, size_t size );

    protected:
        std::string bucket;
        std::string object;
};

class AmazonS3Head : public AmazonRequest {
    public:
        AmazonS3Head(
            const std::string & s,
            const std::string & akf,
            const std::string & skf,
            const std::string & b,
            const std::string & o
        ) :
            AmazonRequest(s, akf, skf),
            bucket(b),
            object(o)
        { }

        virtual ~AmazonS3Head();

        virtual bool SendRequest();

    protected:
        std::string bucket;
        std::string object;
};

#endif /* S3_COMMANDS_H */
