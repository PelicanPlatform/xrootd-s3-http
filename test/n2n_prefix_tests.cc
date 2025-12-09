/***************************************************************
 *
 * Copyright (C) 2025, Pelican Project, Morgridge Institute for Research
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

#include "../src/PrefixN2N.hh"
#include "../src/shortfile.hh"

#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>

using namespace XrdHTTPServer;

class PrefixN2NTest : public testing::Test {
  protected:
	PrefixN2NTest()
		: m_log(new XrdSysLogger(2, 0)), m_err(m_log.get(), "test_") {}

	void SetUp() override { setenv("XRDINSTANCE", "xrootd", 1); }

	std::unique_ptr<XrdSysLogger> m_log;
	XrdSysError m_err;
};

// Test: Basic path prefix matching
TEST_F(PrefixN2NTest, BasicPathPrefixMatch) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	// Test exact match
	EXPECT_EQ(0, n2n.lfn2pfn("/store", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms", buff);

	// Test prefix match with subpath
	EXPECT_EQ(0, n2n.lfn2pfn("/store/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/file.txt", buff);

	// Test deeper path
	EXPECT_EQ(0, n2n.lfn2pfn("/store/subdir/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/subdir/file.txt", buff);
}

// Test: Path boundary matching (should NOT match substring prefixes)
TEST_F(PrefixN2NTest, PathBoundaryMatching) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/foo", "/bar");

	char buff[1024];

	// Should match /foo
	EXPECT_EQ(0, n2n.lfn2pfn("/foo", buff, sizeof(buff)));
	EXPECT_STREQ("/bar", buff);

	// Should match /foo/subdir
	EXPECT_EQ(0, n2n.lfn2pfn("/foo/subdir", buff, sizeof(buff)));
	EXPECT_STREQ("/bar/subdir", buff);

	// Should NOT match /foobar (not a path boundary)
	EXPECT_EQ(0, n2n.lfn2pfn("/foobar", buff, sizeof(buff)));
	EXPECT_STREQ("/foobar", buff); // Unchanged

	// Should NOT match /foobar/baz
	EXPECT_EQ(0, n2n.lfn2pfn("/foobar/baz", buff, sizeof(buff)));
	EXPECT_STREQ("/foobar/baz", buff); // Unchanged
}

// Test: Reverse mapping (pfn2lfn)
TEST_F(PrefixN2NTest, ReverseMapping) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	// Test reverse mapping
	EXPECT_EQ(0, n2n.pfn2lfn("/data/cms", buff, sizeof(buff)));
	EXPECT_STREQ("/store", buff);

	EXPECT_EQ(0, n2n.pfn2lfn("/data/cms/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/store/file.txt", buff);

	EXPECT_EQ(0, n2n.pfn2lfn("/data/cms/subdir/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/store/subdir/file.txt", buff);
}

// Test: Multiple rules (first match wins)
TEST_F(PrefixN2NTest, MultipleRules) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store/mc", "/data/mc");
	n2n.addRule("/store/data", "/data/physics");
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	// First rule matches
	EXPECT_EQ(0, n2n.lfn2pfn("/store/mc/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/mc/file.txt", buff);

	// Second rule matches
	EXPECT_EQ(0, n2n.lfn2pfn("/store/data/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/physics/file.txt", buff);

	// Third rule matches (more general)
	EXPECT_EQ(0, n2n.lfn2pfn("/store/other/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/other/file.txt", buff);
}

// Test: No matching rule (passthrough)
TEST_F(PrefixN2NTest, NoMatchingRule) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	// Path that doesn't match any rule should pass through unchanged
	EXPECT_EQ(0, n2n.lfn2pfn("/other/path/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/other/path/file.txt", buff);
}

// Test: Trailing slashes handling
TEST_F(PrefixN2NTest, TrailingSlashNormalization) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store/", "/data/cms/"); // Rules with trailing slashes

	char buff[1024];

	// Should still work (trailing slashes in rules normalized)
	EXPECT_EQ(0, n2n.lfn2pfn("/store", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms", buff);

	EXPECT_EQ(0, n2n.lfn2pfn("/store/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/file.txt", buff);

	// Input with trailing slash should preserve it in output
	EXPECT_EQ(0, n2n.lfn2pfn("/store/", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/", buff);

	// Trailing slash on subpath should also be preserved
	EXPECT_EQ(0, n2n.lfn2pfn("/store/subdir/", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/subdir/", buff);
}

// Test: Buffer too small
TEST_F(PrefixN2NTest, BufferTooSmall) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/s", "/very/long/destination/path");

	char buff[10];

	// Result is too long for buffer
	EXPECT_EQ(ENAMETOOLONG, n2n.lfn2pfn("/s/file.txt", buff, sizeof(buff)));
}

// Test: Empty path
TEST_F(PrefixN2NTest, EmptyPath) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data");

	char buff[1024];

	// Empty path should return root
	EXPECT_EQ(0, n2n.lfn2pfn("", buff, sizeof(buff)));
	EXPECT_STREQ("/", buff);
}

// Test: Root prefix
TEST_F(PrefixN2NTest, RootPrefix) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/", "/data");

	char buff[1024];

	// Root prefix matches everything
	EXPECT_EQ(0, n2n.lfn2pfn("/", buff, sizeof(buff)));
	EXPECT_STREQ("/data", buff);

	EXPECT_EQ(0, n2n.lfn2pfn("/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/file.txt", buff);

	EXPECT_EQ(0, n2n.lfn2pfn("/subdir/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/subdir/file.txt", buff);
}

// Test: lfn2rfn (should be same as lfn2pfn)
TEST_F(PrefixN2NTest, Lfn2Rfn) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	EXPECT_EQ(0, n2n.lfn2rfn("/store/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/file.txt", buff);
}

// Test: Configuration via parameter string
TEST_F(PrefixN2NTest, ConfigurationViaParams) {
	// Create with parameter string
	PrefixN2N n2n(&m_err, nullptr, "/store /data/cms /cache /tmp/cache",
				  nullptr);

	char buff[1024];

	// First rule from params
	EXPECT_EQ(0, n2n.lfn2pfn("/store/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/file.txt", buff);

	// Second rule from params
	EXPECT_EQ(0, n2n.lfn2pfn("/cache/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/tmp/cache/file.txt", buff);
}

// Test: Get rules (for inspection)
TEST_F(PrefixN2NTest, GetRules) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");
	n2n.addRule("/cache", "/tmp/cache", true); // strict mode

	const auto &rules = n2n.getRules();
	EXPECT_EQ(2u, rules.size());
	EXPECT_EQ("/store", rules[0].matchPrefix);
	EXPECT_EQ("/data/cms", rules[0].substitutePrefix);
	EXPECT_FALSE(rules[0].strict);
	EXPECT_EQ("/cache", rules[1].matchPrefix);
	EXPECT_EQ("/tmp/cache", rules[1].substitutePrefix);
	EXPECT_TRUE(rules[1].strict);
}

// Test: Deep nesting
TEST_F(PrefixN2NTest, DeepNesting) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/a/b/c/d", "/x/y/z");

	char buff[1024];

	EXPECT_EQ(0, n2n.lfn2pfn("/a/b/c/d/e/f/g.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/x/y/z/e/f/g.txt", buff);

	// Partial match should not work
	EXPECT_EQ(0, n2n.lfn2pfn("/a/b/c/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/a/b/c/file.txt", buff); // Unchanged
}

// Test: Null and invalid inputs
TEST_F(PrefixN2NTest, NullInputs) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);

	char buff[1024];

	// Null buffer should return error
	EXPECT_EQ(EINVAL, n2n.lfn2pfn("/test", nullptr, sizeof(buff)));

	// Zero buffer length should return error
	EXPECT_EQ(EINVAL, n2n.lfn2pfn("/test", buff, 0));

	// Null path should return error
	EXPECT_EQ(EINVAL, n2n.lfn2pfn(nullptr, buff, sizeof(buff)));
}

// Test: Roundtrip (lfn -> pfn -> lfn)
TEST_F(PrefixN2NTest, Roundtrip) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff1[1024], buff2[1024];

	const char *originalLfn = "/store/subdir/file.txt";

	// LFN -> PFN
	EXPECT_EQ(0, n2n.lfn2pfn(originalLfn, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms/subdir/file.txt", buff1);

	// PFN -> LFN (roundtrip)
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(originalLfn, buff2);
}

// Test: JSON string parsing - basic
TEST_F(PrefixN2NTest, JsonStringParseBasic) {
	auto [success, result] = PrefixN2N::parseJsonString("\"hello world\"");
	EXPECT_TRUE(success);
	EXPECT_EQ("hello world", result);
}

// Test: JSON string parsing - escape sequences
TEST_F(PrefixN2NTest, JsonStringParseEscapes) {
	// Test quote escape
	auto [s1, r1] = PrefixN2N::parseJsonString("\"hello \\\"world\\\"\"");
	EXPECT_TRUE(s1);
	EXPECT_EQ("hello \"world\"", r1);

	// Test backslash escape
	auto [s2, r2] = PrefixN2N::parseJsonString("\"path\\\\to\\\\file\"");
	EXPECT_TRUE(s2);
	EXPECT_EQ("path\\to\\file", r2);

	// Test newline and tab
	auto [s3, r3] = PrefixN2N::parseJsonString("\"line1\\nline2\\ttabbed\"");
	EXPECT_TRUE(s3);
	EXPECT_EQ("line1\nline2\ttabbed", r3);
}

// Test: JSON string parsing - unicode escape
TEST_F(PrefixN2NTest, JsonStringParseUnicode) {
	// ASCII range unicode
	auto [s1, r1] = PrefixN2N::parseJsonString("\"\\u0041\\u0042\\u0043\"");
	EXPECT_TRUE(s1);
	EXPECT_EQ("ABC", r1);

	// Space character
	auto [s2, r2] = PrefixN2N::parseJsonString("\"hello\\u0020world\"");
	EXPECT_TRUE(s2);
	EXPECT_EQ("hello world", r2);
}

// Test: JSON string parsing - invalid inputs
TEST_F(PrefixN2NTest, JsonStringParseInvalid) {
	// Not starting with quote
	auto [s1, r1] = PrefixN2N::parseJsonString("hello");
	EXPECT_FALSE(s1);

	// Unterminated string
	auto [s2, r2] = PrefixN2N::parseJsonString("\"hello");
	EXPECT_FALSE(s2);

	// Invalid escape
	auto [s3, r3] = PrefixN2N::parseJsonString("\"hello\\x\"");
	EXPECT_FALSE(s3);

	// Null input
	auto [s4, r4] = PrefixN2N::parseJsonString(nullptr);
	EXPECT_FALSE(s4);
}

// Test: Paths with spaces using addRule
TEST_F(PrefixN2NTest, PathsWithSpaces) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/path with spaces", "/destination with spaces");

	char buff[1024];

	// Exact match
	EXPECT_EQ(0, n2n.lfn2pfn("/path with spaces", buff, sizeof(buff)));
	EXPECT_STREQ("/destination with spaces", buff);

	// With subpath
	EXPECT_EQ(0, n2n.lfn2pfn("/path with spaces/subdir/file.txt", buff,
							 sizeof(buff)));
	EXPECT_STREQ("/destination with spaces/subdir/file.txt", buff);

	// Reverse mapping
	EXPECT_EQ(0, n2n.pfn2lfn("/destination with spaces/file.txt", buff,
							 sizeof(buff)));
	EXPECT_STREQ("/path with spaces/file.txt", buff);
}

// Test: Paths with spaces - boundary matching
TEST_F(PrefixN2NTest, PathsWithSpacesBoundary) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/my path", "/target");

	char buff[1024];

	// Should NOT match "/my pathextra" (not a path boundary)
	EXPECT_EQ(0, n2n.lfn2pfn("/my pathextra", buff, sizeof(buff)));
	EXPECT_STREQ("/my pathextra", buff); // Unchanged

	// Should match "/my path/extra"
	EXPECT_EQ(0, n2n.lfn2pfn("/my path/extra", buff, sizeof(buff)));
	EXPECT_STREQ("/target/extra", buff);
}

// Test: Configuration from file with quoted paths (spaces)
TEST_F(PrefixN2NTest, ConfigFileWithQuotedPaths) {
	// Create a temp config file
	char tmp_configfn[] = "/tmp/prefixn2n-test.cfg.XXXXXX";
	auto fd = mkstemp(tmp_configfn);
	ASSERT_NE(fd, -1) << "Failed to create temp file";
	close(fd);

	// Note: For paths with spaces, use JSON-style quoted strings
	// XRootD config file processing may consume some escape sequences
	std::string config =
		"prefixn2n.rule \"/source with spaces\" \"/dest with spaces\"\n"
		"prefixn2n.rule /simple /target\n"
		"prefixn2n.rule \"/path/with multiple/spaces\" \"/destination/with "
		"spaces\"\n";

	ASSERT_TRUE(writeShortFile(tmp_configfn, config, 0));

	PrefixN2N n2n(&m_err, tmp_configfn, nullptr, nullptr);

	char buff[1024];

	// Test rule with spaces
	EXPECT_EQ(0,
			  n2n.lfn2pfn("/source with spaces/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/dest with spaces/file.txt", buff);

	// Test simple rule
	EXPECT_EQ(0, n2n.lfn2pfn("/simple/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/target/file.txt", buff);

	// Test rule with multiple spaces in different path components
	EXPECT_EQ(0, n2n.lfn2pfn("/path/with multiple/spaces/subdir/file.txt", buff,
							 sizeof(buff)));
	EXPECT_STREQ("/destination/with spaces/subdir/file.txt", buff);

	// Cleanup
	unlink(tmp_configfn);
}

// Test: Non-strict mode normalizes multiple consecutive slashes
TEST_F(PrefixN2NTest, SlashNormalizationNonStrict) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms"); // non-strict (default)

	char buff[1024];

	// Multiple slashes in input should be normalized to single slashes
	EXPECT_EQ(0, n2n.lfn2pfn("/store//file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/file.txt", buff);

	EXPECT_EQ(0, n2n.lfn2pfn("/store///subdir//file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/subdir/file.txt", buff);

	// Multiple slashes at boundary
	EXPECT_EQ(0, n2n.lfn2pfn("/store//", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/", buff);
}

// Test: Strict mode preserves multiple consecutive slashes
TEST_F(PrefixN2NTest, SlashPreservationStrict) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms", true); // strict mode

	char buff[1024];

	// Multiple slashes in input should be preserved
	EXPECT_EQ(0, n2n.lfn2pfn("/store//file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms//file.txt", buff);

	EXPECT_EQ(0, n2n.lfn2pfn("/store///subdir//file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms///subdir//file.txt", buff);
}

// Test: Config file with -strict flag
TEST_F(PrefixN2NTest, ConfigFileWithStrictFlag) {
	// Create a temp config file
	char tmp_configfn[] = "/tmp/prefixn2n-strict-test.cfg.XXXXXX";
	auto fd = mkstemp(tmp_configfn);
	ASSERT_NE(fd, -1) << "Failed to create temp file";
	close(fd);

	std::string config = "prefixn2n.rule /normal /target1\n"
						 "prefixn2n.rule -strict /strict /target2\n";

	ASSERT_TRUE(writeShortFile(tmp_configfn, config, 0));

	PrefixN2N n2n(&m_err, tmp_configfn, nullptr, nullptr);

	const auto &rules = n2n.getRules();
	ASSERT_EQ(2u, rules.size());
	EXPECT_FALSE(rules[0].strict);
	EXPECT_TRUE(rules[1].strict);

	char buff[1024];

	// Non-strict rule normalizes slashes
	EXPECT_EQ(0, n2n.lfn2pfn("/normal//file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/target1/file.txt", buff);

	// Strict rule preserves slashes
	EXPECT_EQ(0, n2n.lfn2pfn("/strict//file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/target2//file.txt", buff);

	// Cleanup
	unlink(tmp_configfn);
}

// Test: Trailing slash preservation with no matching rule
TEST_F(PrefixN2NTest, TrailingSlashNoMatch) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	// Path that doesn't match any rule should preserve trailing slash
	EXPECT_EQ(0, n2n.lfn2pfn("/other/path/", buff, sizeof(buff)));
	EXPECT_STREQ("/other/path/", buff);
}

// Test: Empty path behavior
TEST_F(PrefixN2NTest, EmptyPathTrailingSlash) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/", "/data");

	char buff[1024];

	// Root slash shouldn't be treated as having "trailing slash"
	EXPECT_EQ(0, n2n.lfn2pfn("/", buff, sizeof(buff)));
	EXPECT_STREQ("/data", buff);
}

// Test: Reverse mapping preserves trailing slash
TEST_F(PrefixN2NTest, ReverseTrailingSlash) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms");

	char buff[1024];

	// Forward with trailing slash
	EXPECT_EQ(0, n2n.lfn2pfn("/store/subdir/", buff, sizeof(buff)));
	EXPECT_STREQ("/data/cms/subdir/", buff);

	// Reverse with trailing slash
	EXPECT_EQ(0, n2n.pfn2lfn("/data/cms/subdir/", buff, sizeof(buff)));
	EXPECT_STREQ("/store/subdir/", buff);
}

// Test: Strict mode with reverse mapping
TEST_F(PrefixN2NTest, StrictReverseMapping) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data//cms", true); // strict mode, dest has //

	char buff[1024];

	// Forward: should preserve the // in destination
	EXPECT_EQ(0, n2n.lfn2pfn("/store/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/data//cms/file.txt", buff);

	// Reverse: should match path with //
	EXPECT_EQ(0, n2n.pfn2lfn("/data//cms/file.txt", buff, sizeof(buff)));
	EXPECT_STREQ("/store/file.txt", buff);
}

// Test: Strict mode roundtrip - basic paths
TEST_F(PrefixN2NTest, StrictRoundtripBasic) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms", true); // strict mode

	char buff1[1024], buff2[1024];

	// Simple path roundtrip
	const char *lfn1 = "/store/file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn1, buff1, sizeof(buff1)));
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn1, buff2);

	// Nested path roundtrip
	const char *lfn2 = "/store/subdir/nested/file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn2, buff1, sizeof(buff1)));
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn2, buff2);

	// Exact prefix match roundtrip
	const char *lfn3 = "/store";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn3, buff1, sizeof(buff1)));
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn3, buff2);
}

// Test: Strict mode roundtrip - trailing slashes preserved
TEST_F(PrefixN2NTest, StrictRoundtripTrailingSlash) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms", true); // strict mode

	char buff1[1024], buff2[1024];

	// Path with trailing slash
	const char *lfn1 = "/store/subdir/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn1, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms/subdir/", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn1, buff2);

	// Exact prefix with trailing slash
	const char *lfn2 = "/store/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn2, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms/", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn2, buff2);

	// Deeply nested with trailing slash
	const char *lfn3 = "/store/a/b/c/d/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn3, buff1, sizeof(buff1)));
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn3, buff2);
}

// Test: Strict mode roundtrip - double slashes preserved
TEST_F(PrefixN2NTest, StrictRoundtripDoubleSlashes) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms", true); // strict mode

	char buff1[1024], buff2[1024];

	// Path with double slashes in middle
	const char *lfn1 = "/store//file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn1, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms//file.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn1, buff2);

	// Path with multiple double slashes
	const char *lfn2 = "/store//subdir//file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn2, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms//subdir//file.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn2, buff2);

	// Path with triple slashes
	const char *lfn3 = "/store///file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn3, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms///file.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn3, buff2);
}

// Test: Strict mode roundtrip - double slashes AND trailing slash
TEST_F(PrefixN2NTest, StrictRoundtripDoubleSlashesAndTrailing) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/store", "/data/cms", true); // strict mode

	char buff1[1024], buff2[1024];

	// Double slashes with trailing slash
	const char *lfn1 = "/store//subdir/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn1, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/cms//subdir/", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn1, buff2);

	// Multiple double slashes with trailing
	const char *lfn2 = "/store//a//b/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn2, buff1, sizeof(buff1)));
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn2, buff2);
}

// Test: Strict mode roundtrip - root prefix
TEST_F(PrefixN2NTest, StrictRoundtripRootPrefix) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/", "/data", true); // strict mode, root prefix

	char buff1[1024], buff2[1024];

	// Root path itself (special case - no trailing slash to preserve)
	const char *lfn1 = "/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn1, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn1, buff2);

	// Simple file under root
	const char *lfn2 = "/file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn2, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/file.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn2, buff2);

	// Nested path under root
	const char *lfn3 = "/subdir/file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn3, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/subdir/file.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn3, buff2);

	// Path with trailing slash under root
	const char *lfn4 = "/subdir/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn4, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/subdir/", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn4, buff2);

	// Path with leading double slashes under root
	// Note: With root prefix "/", input "//file.txt" = prefix "/" + suffix
	// "/file.txt" The leading // is split as: one / is the prefix, one / starts
	// the suffix So //file.txt -> /data/file.txt (the double slash is NOT
	// preserved) This is because "/" consumes only one slash as the prefix
	const char *lfn5 = "//file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn5, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data/file.txt", buff1);
	// Reverse: /data/file.txt -> /file.txt (single leading slash)
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ("/file.txt", buff2);
	// Note: This is NOT a perfect roundtrip for //file.txt with root prefix
}

// Test: Strict mode roundtrip - paths with spaces
TEST_F(PrefixN2NTest, StrictRoundtripPathsWithSpaces) {
	PrefixN2N n2n(&m_err, nullptr, nullptr, nullptr);
	n2n.addRule("/my store", "/data storage", true); // strict mode

	char buff1[1024], buff2[1024];

	// Simple path with spaces
	const char *lfn1 = "/my store/file name.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn1, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data storage/file name.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn1, buff2);

	// Path with spaces and trailing slash
	const char *lfn2 = "/my store/sub dir/";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn2, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data storage/sub dir/", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn2, buff2);

	// Path with spaces and double slashes
	const char *lfn3 = "/my store//file.txt";
	EXPECT_EQ(0, n2n.lfn2pfn(lfn3, buff1, sizeof(buff1)));
	EXPECT_STREQ("/data storage//file.txt", buff1);
	EXPECT_EQ(0, n2n.pfn2lfn(buff1, buff2, sizeof(buff2)));
	EXPECT_STREQ(lfn3, buff2);
}
