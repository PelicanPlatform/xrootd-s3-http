
include(GoogleTest)

add_executable( s3-gtest s3_tests.cc )
add_executable( s3-unit-test s3_unit_tests.cc )
add_executable( http-gtest http_tests.cc )

target_link_libraries(s3-gtest XrdS3Testing GTest::gtest_main Threads::Threads)
target_link_libraries(s3-unit-test XrdS3Testing GTest::gtest_main Threads::Threads)
target_link_libraries(http-gtest XrdHTTPServerTesting GTest::gtest_main Threads::Threads)

gtest_add_tests(TARGET s3-unit-test TEST_LIST s3UnitTests)
set_tests_properties(${s3UnitTests}
  PROPERTIES
    FIXTURES_REQUIRED S3::s3_basic
    ENVIRONMENT "ENV_FILE=${CMAKE_BINARY_DIR}/tests/s3_basic/setup.sh"
)

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

if (VALGRIND)
  add_test(
    NAME
      valgrind-s3
    COMMAND
      ${VALGRIND_BIN} ${CMAKE_CURRENT_BINARY_DIR}/s3-unit-test -R FileSystemS3Fixture.UploadLargePartAligned
  )

  set_tests_properties(valgrind-s3
    PROPERTIES
      FIXTURES_REQUIRED S3::s3_basic
  )
endif()

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
