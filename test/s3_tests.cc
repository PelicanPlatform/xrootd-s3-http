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

#include "../src/S3Commands.hh"

#include <gtest/gtest.h>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>

class TestAmazonRequest : public AmazonRequest {
public:
    XrdSysLogger log{};
    XrdSysError  err{&log, "TestS3CommandsLog"};

    TestAmazonRequest(const std::string& url, const std::string& akf, const std::string& skf,
                      const std::string& bucket, const std::string& object, const std::string& path,
                      int sigVersion)
        : AmazonRequest(url, akf, skf, bucket, object, path, sigVersion, err) {}

    // For getting access to otherwise-protected members
    std::string getHostUrl() const {
        return hostUrl;
    }
};

TEST(TestS3URLGeneration, Test1) {
    const std::string serviceUrl = "https://s3-service.com:443";
    const std::string b = "test-bucket";
    const std::string o = "test-object";

    // Test path-style URL generation
    TestAmazonRequest pathReq{serviceUrl, "akf", "skf", b, o, "path", 4};
    std::string generatedHostUrl = pathReq.getHostUrl();
    ASSERT_EQ(generatedHostUrl, "https://s3-service.com:443/test-bucket/test-object");

    // Test virtual-style URL generation
    TestAmazonRequest virtReq{serviceUrl, "akf", "skf", b, o, "virtual", 4};
    generatedHostUrl = virtReq.getHostUrl();
    ASSERT_EQ(generatedHostUrl, "https://test-bucket.s3-service.com:443/test-object");

    // Test path-style with empty bucket (which we use for exporting an entire endpoint)
    TestAmazonRequest pathReqNoBucket{serviceUrl, "akf", "skf", "", o, "path", 4};
    generatedHostUrl = pathReqNoBucket.getHostUrl();
    ASSERT_EQ(generatedHostUrl, "https://s3-service.com:443/test-object");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
