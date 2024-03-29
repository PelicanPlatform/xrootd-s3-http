//
// Created by Rich Wellner on 2/29/24.
//

#include "S3AccessInfo.hh"

const std::string &S3AccessInfo::getS3BucketName() const {
  return s3_bucket_name;
}

void S3AccessInfo::setS3BucketName(const std::string &s3BucketName) {
  s3_bucket_name = s3BucketName;
}

const std::string &S3AccessInfo::getS3ServiceName() const {
  return s3_service_name;
}

void S3AccessInfo::setS3ServiceName(const std::string &s3ServiceName) {
  s3_service_name = s3ServiceName;
}

const std::string &S3AccessInfo::getS3Region() const { return s3_region; }

void S3AccessInfo::setS3Region(const std::string &s3Region) {
    s3_region = s3Region;
}

const std::string &S3AccessInfo::getS3ServiceUrl() const {
    return s3_service_url;
}

void S3AccessInfo::setS3ServiceUrl(const std::string &s3ServiceUrl) {
    s3_service_url = s3ServiceUrl;
}

const std::string &S3AccessInfo::getS3AccessKeyFile() const {
    return s3_access_key_file;
}

void S3AccessInfo::setS3AccessKeyFile(const std::string &s3AccessKeyFile) {
    s3_access_key_file = s3AccessKeyFile;
}

const std::string &S3AccessInfo::getS3SecretKeyFile() const {
    return s3_secret_key_file;
}

void S3AccessInfo::setS3SecretKeyFile(const std::string &s3SecretKeyFile) {
    s3_secret_key_file = s3SecretKeyFile;
}
