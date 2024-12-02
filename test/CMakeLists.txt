add_executable( s3-gtest s3_tests.cc
  ../src/AWSv4-impl.cc
  ../src/CurlUtil.cc
  ../src/logging.cc
  ../src/S3AccessInfo.cc
  ../src/S3Directory.cc
  ../src/S3File.cc
  ../src/S3FileSystem.cc
  ../src/shortfile.cc
  ../src/stl_string_utils.cc
  ../src/TokenFile.cc
  ../src/HTTPCommands.cc
  ../src/S3Commands.cc
)

add_executable( http-gtest http_tests.cc
  ../src/CurlUtil.cc
  ../src/HTTPFile.cc
  ../src/HTTPFileSystem.cc
  ../src/HTTPCommands.cc
  ../src/stl_string_utils.cc
  ../src/TokenFile.cc
  ../src/shortfile.cc
  ../src/logging.cc
)


if(XROOTD_PLUGINS_EXTERNAL_GTEST)
    set(LIBGTEST "gtest")
else()
    add_dependencies(s3-gtest gtest)
    add_dependencies(http-gtest gtest)
    include_directories("${PROJECT_SOURCE_DIR}/vendor/gtest/googletest/include")
    set(LIBGTEST "${CMAKE_BINARY_DIR}/external/gtest/src/gtest-build/lib/libgtest.a")
endif()

target_link_libraries(s3-gtest XrdS3 "${LIBGTEST}" pthread)
target_link_libraries(http-gtest XrdHTTPServer "${LIBGTEST}" pthread)


add_test(
  NAME
    s3-unit
  COMMAND
    ${CMAKE_CURRENT_BINARY_DIR}/s3-gtest
)

add_test(
  NAME
    http-unit
  COMMAND
    ${CMAKE_CURRENT_BINARY_DIR}/http-gtest "${CMAKE_BINARY_DIR}/tests/basic/setup.sh"
)

set_tests_properties(http-unit
  PROPERTIES
    FIXTURES_REQUIRED HTTP::basic
)

######################################
# Integration tests.
######################################
add_test(NAME HTTP::basic::setup
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/xrdhttp-setup.sh" basic)

set_tests_properties(HTTP::basic::setup
  PROPERTIES
    FIXTURES_SETUP HTTP::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_SOURCE_DIR}"
)

add_test(NAME HTTP::basic::teardown
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/xrdhttp-teardown.sh" basic)

set_tests_properties(HTTP::basic::teardown
  PROPERTIES
    FIXTURES_CLEANUP HTTP::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
)

add_test(NAME HTTP::basic::test
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/xrdhttp-test.sh" basic)

list(APPEND BASIC_TEST_LOGS ${CMAKE_CURRENT_BINARY_DIR}/tests/basic/server.log)
list(APPEND BASIC_TEST_LOGS ${CMAKE_CURRENT_BINARY_DIR}/tests/basic/client.log)

set_tests_properties(HTTP::basic::test
  PROPERTIES
    FIXTURES_REQUIRED HTTP::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
    ATTACHED_FILES_ON_FAIL "${BASIC_TEST_LOGS}"
)

####
#   Start of S3 tests
####
add_test(NAME S3::s3_basic::setup
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-setup.sh" s3_basic)

set_tests_properties(S3::s3_basic::setup
  PROPERTIES
    FIXTURES_SETUP S3::s3_basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_SOURCE_DIR}"
)

add_test(NAME S3::s3_basic::teardown
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-teardown.sh" s3_basic)

set_tests_properties(S3::s3_basic::teardown
  PROPERTIES
    FIXTURES_CLEANUP S3::s3_basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
)

add_test(NAME S3::s3_basic::test
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-test.sh" s3_basic)

add_test(NAME S3::s3_basic::stress_test
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-stress-test.sh" s3_basic)

add_test(NAME S3::s3_basic::list_test
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-list-test.sh" s3_basic)

list(APPEND S3_BASIC_TEST_LOGS ${CMAKE_CURRENT_BINARY_DIR}/tests/s3_basic/server.log)
list(APPEND S3_BASIC_TEST_LOGS ${CMAKE_CURRENT_BINARY_DIR}/tests/s3_basic/client.log)

set_tests_properties(S3::s3_basic::test
  PROPERTIES
    FIXTURES_REQUIRED S3::s3_basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
    ATTACHED_FILES_ON_FAIL "${S3_BASIC_TEST_LOGS}"
)

set_tests_properties(S3::s3_basic::stress_test
  PROPERTIES
    FIXTURES_REQUIRED S3::s3_basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
    ATTACHED_FILES_ON_FAIL "${S3_BASIC_TEST_LOGS}"
)

set_tests_properties(S3::s3_basic::list_test
  PROPERTIES
    FIXTURES_REQUIRED S3::s3_basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
    ATTACHED_FILES_ON_FAIL "${S3_BASIC_TEST_LOGS}"
)
