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
    if (RESULT_CODE EQUAL 0)
      file(CHMOD "${FILE}" PERMISSIONS
             OWNER_READ OWNER_WRITE OWNER_EXECUTE
             GROUP_READ GROUP_EXECUTE
             WORLD_READ WORLD_EXECUTE)
    else()
      file(REMOVE "${FILE}")
      message(WARNING "Failed to download ${EXE}: ${RESULT_CODE}")
    endif()

  endif()

endforeach()

find_program(MINIO_BIN minio HINTS "${CMAKE_CURRENT_BINARY_DIR}")
find_program(MC_BIN mc       HINTS "${CMAKE_CURRENT_BINARY_DIR}")
if (NOT MINIO_BIN OR NOT MC_BIN)
  message(WARNING "minio and/or mc are missing, will skip many s3 tests")
endif()

include(GoogleTest)

add_executable( s3-gtest s3_tests.cc )
add_executable( s3-unit-test s3_unit_tests.cc )
add_executable( http-gtest http_tests.cc )
add_executable( filter-gtest filter_tests.cc ../src/shortfile.cc )
add_executable( posc-gtest posc_tests.cc ../src/shortfile.cc )
add_executable( n2n-prefix-gtest n2n_prefix_tests.cc ../src/shortfile.cc )

target_link_libraries(s3-gtest XrdS3Testing GTest::gtest_main Threads::Threads)
target_link_libraries(s3-unit-test XrdS3Testing GTest::gtest_main Threads::Threads)
target_link_libraries(http-gtest XrdHTTPServerTesting GTest::gtest_main Threads::Threads)
target_link_libraries(filter-gtest XrdOssFilterTesting GTest::gtest_main Threads::Threads)
target_link_libraries(posc-gtest XrdOssPoscTesting GTest::gtest_main Threads::Threads)
target_link_libraries(n2n-prefix-gtest XrdN2NPrefixTesting GTest::gtest_main Threads::Threads)

gtest_add_tests(TARGET filter-gtest TEST_LIST filterUnitTests)

set_tests_properties(${filterUnitTests}
  PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan-suppressions.txt"
)

gtest_add_tests(TARGET n2n-prefix-gtest TEST_LIST n2nPrefixUnitTests)

set_tests_properties(${n2nPrefixUnitTests}
  PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan-suppressions.txt"
)

if (MINIO_BIN AND MC_BIN)
  gtest_add_tests(TARGET s3-unit-test TEST_LIST s3UnitTests)
  set_tests_properties(${s3UnitTests}
    PROPERTIES
      FIXTURES_REQUIRED S3::s3_basic
      ENVIRONMENT "ENV_FILE=${CMAKE_BINARY_DIR}/tests/s3_basic/setup.sh;LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan-suppressions.txt"
  )
endif()

gtest_add_tests(TARGET posc-gtest TEST_LIST poscUnitTests)

set_tests_properties(${poscUnitTests}
  PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan-suppressions.txt"
)

add_test(
  NAME
    s3-unit
  COMMAND
    ${CMAKE_CURRENT_BINARY_DIR}/s3-gtest
)
set_tests_properties(s3-unit
  PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan-suppressions.txt"
)

gtest_add_tests(TARGET http-gtest TEST_LIST httpUnitTests)

set_tests_properties( ${httpUnitTests}
  PROPERTIES
    ENVIRONMENT "ENV_FILE=${CMAKE_CURRENT_BINARY_DIR}/../tests/basic/setup.sh;LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan-suppressions.txt"
    FIXTURES_REQUIRED HTTP::basic
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

######################################
# Integration tests.
######################################
add_test(NAME HTTP::basic::setup
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/xrdhttp-setup.sh" basic)

set_tests_properties(HTTP::basic::setup
  PROPERTIES
    FIXTURES_SETUP HTTP::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_SOURCE_DIR};XROOTD_BINDIR=${XRootD_DATA_DIR}/../bin;LD_LIBRARY_PATH=${XRootD_LIB_DIR}"
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

######################################
# N2N (Name-to-Name) Integration Tests
######################################
add_test(NAME N2N::basic::setup
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/n2n-setup.sh" n2n_basic)

set_tests_properties(N2N::basic::setup
  PROPERTIES
    FIXTURES_SETUP N2N::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_SOURCE_DIR};XROOTD_BINDIR=${XRootD_DATA_DIR}/../bin;LD_LIBRARY_PATH=${XRootD_LIB_DIR}:${CMAKE_BINARY_DIR}/lib"
)

add_test(NAME N2N::basic::teardown
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/n2n-teardown.sh" n2n_basic)

set_tests_properties(N2N::basic::teardown
  PROPERTIES
    FIXTURES_CLEANUP N2N::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
)

add_test(NAME N2N::basic::test
  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/n2n-test.sh" n2n_basic)

list(APPEND N2N_BASIC_TEST_LOGS ${CMAKE_BINARY_DIR}/tests/n2n_basic/server.log)
list(APPEND N2N_BASIC_TEST_LOGS ${CMAKE_BINARY_DIR}/tests/n2n_basic/client.log)

set_tests_properties(N2N::basic::test
  PROPERTIES
    FIXTURES_REQUIRED N2N::basic
    ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR}"
    ATTACHED_FILES_ON_FAIL "${N2N_BASIC_TEST_LOGS}"
)

####
#   Start of S3 tests, which require minio and mc
####
if (MINIO_BIN AND MC_BIN)
  add_test(NAME S3::s3_basic::setup
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-setup.sh" s3_basic)

  set_tests_properties(S3::s3_basic::setup
    PROPERTIES
      FIXTURES_SETUP S3::s3_basic
      ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR};XROOTD_BINDIR=${XRootD_DATA_DIR}/../bin;LD_LIBRARY_PATH=${XRootD_LIB_DIR};MINIO_BIN=${MINIO_BIN};MC_BIN=${MC_BIN}"
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

  if( GoWrk )
  add_test( NAME S3::s3_basic::gowrk_test
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/s3-gowrk-test.sh" s3_basic )

  set_tests_properties( S3::s3_basic::gowrk_test
    PROPERTIES
      FIXTURES_REQUIRED S3::s3_basic
      ENVIRONMENT "BINARY_DIR=${CMAKE_BINARY_DIR};WRK_BIN=${GoWrk}"
      ATTACHED_FILES_ON_FAIL "${S3_BASIC_TEST_LOGS}"
  )
  endif()

endif()
