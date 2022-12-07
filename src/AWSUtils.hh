
class AWSCredential {

public:
    AWSCredential(const std::string &accessKeyID, const std::string &secretAccessKey,
                  const std::string &securityToken)
    : m_access_key(accessKeyID),
      m_secret_key(secretAccessKey),
      m_security_token(securityToken)
    {}

    bool
    presign(const std::string &input_region, const std::string &bucket,
        const std::string &object, const std::string &verb,
        std::string &presignedURL, std::string &err);

private:
    const std::string m_access_key;
    const std::string m_secret_key;
    const std::string m_security_token;
};
