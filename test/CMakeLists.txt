set(EXECUTABLES "minio" "mc")

foreach(EXE IN LISTS EXECUTABLES)

  find_program(EXE_BIN ${EXE} HINTS "${CMAKE_CURRENT_BINARY_DIR}")

  if(NOT EXE_BIN)
    message("Didnt find ${EXE}. Downloading it ...")

    if (APPLE)
      set(SYS_NAME "darwin")
    else()
      set(SYS_NAME "linux")
    endif()

    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64" OR
       CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
      set(SYS_PROC "arm64")
    else()
      set(SYS_PROC "amd64")
    endif()

    if (EXE STREQUAL "minio")
      set(DIR "server")
    else()
      set(DIR "client")
    endif()

    set(URL "https://dl.min.io/${DIR}/${EXE}/release/${SYS_NAME}-${SYS_PROC}/${EXE}")
    set(FILE "${CMAKE_CURRENT_BINARY_DIR}/${EXE}")
    message("Downloading ${URL} to ${FILE}")
    file(DOWNLOAD "${URL}" "${FILE}" STATUS download_status)

    list(GET download_status 0 RESULT_CODE)
    if (NOT RESULT_CODE EQUAL 0)
      message(FATAL_ERROR "Failed to download ${EXE}: ${RESULT_CODE}")
    endif()

    file(CHMOD "${FILE}" PERMISSIONS
          OWNER_READ OWNER_WRITE OWNER_EXECUTE
          GROUP_READ GROUP_EXECUTE
          WORLD_READ WORLD_EXECUTE)
  endif()

endforeach()

find_program(MINIO_BIN minio HINTS "${CMAKE_CURRENT_BINARY_DIR}" REQUIRED)
find_program(MC_BIN mc       HINTS "${CMAKE_CURRENT_BINARY_DIR}" REQUIRED)

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
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_SOURCE_DIR};XROOTD_BINDIR=${XRootD_DATA_DIR}/../bin"
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
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_SOURCE_DIR};XROOTD_BINDIR=${XRootD_DATA_DIR}/../bin"
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

#######################################
# Stress-test using the go-wrk binary #
#######################################

add_test( NAME S3::s3_basic::gowrk_test
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-gowrk-test.sh" s3_basic )

set_tests_properties( S3::s3_basic::gowrk_test
  PROPERTIES
    FIXTURES_REQUIRED S3::s3_basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};WRK_BIN=${GoWrk}"
    ATTACHED_FILES_ON_FAIL "${S3_BASIC_TEST_LOGS}"
)
