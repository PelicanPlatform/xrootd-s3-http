//
// Created by Rich Wellner on 2/29/24.
//

#ifndef XROOTD_S3_HTTP_S3ACCESSINFO_HH
#define XROOTD_S3_HTTP_S3ACCESSINFO_HH


#include <string>

class S3AccessInfo {
public:
    const std::string &getS3BucketName() const;

    void setS3BucketName(const std::string &s3BucketName);

    const std::string &getS3ServiceName() const;

    void setS3ServiceName(const std::string &s3ServiceName);

    const std::string &getS3Region() const;

    void setS3Region(const std::string &s3Region);

    const std::string &getS3ServiceUrl() const;

    void setS3ServiceUrl(const std::string &s3ServiceUrl);

    const std::string &getS3AccessKeyFile() const;

    void setS3AccessKeyFile(const std::string &s3AccessKeyFile);

    const std::string &getS3SecretKeyFile() const;

    void setS3SecretKeyFile(const std::string &s3SecretKeyFile);

private:
    std::string s3_bucket_name;
    std::string s3_service_name;
    std::string s3_region;
    std::string s3_service_url;
    std::string s3_access_key_file;
    std::string s3_secret_key_file;
};

#endif //XROOTD_S3_HTTP_S3ACCESSINFO_HH
