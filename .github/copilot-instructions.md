# GitHub Copilot Instructions for xrootd-s3-http

## Project Overview

This repository contains XRootD filesystem plugins that extend XRootD's Open Storage System (OSS) layer. These plugins allow XRootD to serve objects from various backend storage systems.

### Plugins in this Repository

1. **XrdOssHttp** (`src/HTTP*.cc/hh`): Exposes a backend HTTP(S) server as storage
2. **XrdOssS3** (`src/S3*.cc/hh`): Exposes S3-compatible storage as a backend
3. **XrdOssFilter** (`src/Filter.cc/hh`): A stacking plugin that filters files/directories by Unix globs
4. **XrdOssPosc** (`src/Posc.cc/hh`): A stacking plugin for "Persist on Successful Close" functionality
5. **XrdN2NPrefix** (`src/PrefixN2N.cc/hh`): A Name2Name plugin for path prefix translation

## Development Environment

### Prerequisites
- CMake >= 3.14
- C++17 compiler (gcc >= 8 or clang)
- XRootD development headers and libraries
- libcurl, OpenSSL, pthreads

### Building

```bash
mkdir build && cd build
cmake -DBUILD_TESTING=ON ..
make
```

For development with address sanitizer:
```bash
cmake -DBUILD_TESTING=ON -DENABLE_ASAN=ON ..
```

### Running Tests

```bash
cd build
ctest                           # Run all tests
ctest -R <pattern>              # Run tests matching pattern
ctest --output-on-failure       # Show output for failed tests
ctest -E "s3_basic|S3"          # Exclude S3 tests (require MinIO)
```

## Code Style Guidelines

### Formatting
- **Style**: LLVM-based with modifications
- **Indentation**: Tabs (width 4)
- **Tool**: clang-format (v18.1.6)

Run `pre-commit run --all-files` before committing to auto-format code.

### Naming Conventions
- Classes: `PascalCase` (e.g., `PrefixN2N`, `S3FileSystem`)
- Methods: `camelCase` (e.g., `lfn2pfn`, `addRule`)
- Member variables: `m_` prefix (e.g., `m_rules`, `m_eDest`)
- Constants: `UPPER_SNAKE_CASE`

### File Organization
- Headers in `src/` with `.hh` extension
- Implementation in `src/` with `.cc` extension
- Tests in `test/` directory
- Integration test scripts: `*-setup.sh`, `*-test.sh`, `*-teardown.sh`

## XRootD Plugin Development

### OSS Plugin Interface
OSS plugins implement the `XrdOss` interface for storage operations:
- `Stat()`, `Open()`, `Close()`, `Read()`, `Write()`
- Directory operations: `Opendir()`, `Readdir()`, `Closedir()`

### Name2Name (N2N) Plugin Interface
N2N plugins implement `XrdOucName2Name` for path translation:
- `lfn2pfn()`: Logical to physical filename (must include localroot)
- `pfn2lfn()`: Physical to logical filename (strip localroot first)
- `lfn2rfn()`: Logical to remote filename (no localroot)

**Important**: When `oss.namelib` is used, the N2N plugin receives `lroot` (localroot) and must prepend it to physical paths in `lfn2pfn()`.

### Configuration Parsing
Use `XrdOucGatherConf` with `full_lines` mode for custom directive parsing:
```cpp
XrdOucGatherConf conf("myplugin.directive", eDest);
conf.Gather(configfn, XrdOucGatherConf::full_lines);
```

### Logging
Use `XrdSysError` for logging:
```cpp
m_eDest->Say("Message: ", value1, " ", value2);  // Up to 6 args
```

## Testing Patterns

### Unit Tests (GTest)
Located in `test/*_tests.cc`. Example pattern:
```cpp
TEST(TestSuite, TestName) {
    XrdSysLogger logger;
    XrdSysError err(&logger, "test");
    MyPlugin plugin(&err, nullptr, nullptr, nullptr);
    // Test assertions
}
```

### Integration Tests
Three-script pattern with CMake fixtures:
1. `*-setup.sh`: Start XRootD daemon with test configuration
2. `*-test.sh`: Run tests against the daemon
3. `*-teardown.sh`: Stop daemon and cleanup

CMake configuration:
```cmake
add_test(NAME MyTest::setup COMMAND ${CMAKE_SOURCE_DIR}/test/my-setup.sh my_test)
add_test(NAME MyTest::test COMMAND ${CMAKE_SOURCE_DIR}/test/my-test.sh my_test)
add_test(NAME MyTest::teardown COMMAND ${CMAKE_SOURCE_DIR}/test/my-teardown.sh my_test)
set_tests_properties(MyTest::test PROPERTIES FIXTURES_REQUIRED MyTest_fixture)
set_tests_properties(MyTest::setup PROPERTIES FIXTURES_SETUP MyTest_fixture)
set_tests_properties(MyTest::teardown PROPERTIES FIXTURES_CLEANUP MyTest_fixture)
```

## Common Patterns

### Error Handling
- Return `0` for success, `errno` values for errors (e.g., `EINVAL`, `ENAMETOOLONG`)
- Check for null pointers before dereferencing
- Use RAII for resource management

### Buffer Handling
```cpp
if (static_cast<int>(result.size()) >= blen) {
    return ENAMETOOLONG;
}
std::strncpy(buff, result.c_str(), blen);
buff[blen - 1] = '\0';  // Ensure null termination
```

### Path Normalization
- Remove trailing slashes for matching (except root "/")
- Preserve trailing slashes in output if input had them
- Handle consecutive slashes (`//`) based on strict mode

## Dependencies

External dependencies are fetched automatically if not found:
- **nlohmann/json**: JSON parsing (3.11.2)
- **tinyxml2**: XML parsing (10.0.0)
- **GTest**: Unit testing framework

## CI/CD

GitHub Actions workflows:
- `test.yml`: Builds and runs tests on Ubuntu
- `linter.yml`: Runs pre-commit hooks

The CI uses XRootD from `PelicanPlatform/xrootd` fork with Pelican-specific patches.
