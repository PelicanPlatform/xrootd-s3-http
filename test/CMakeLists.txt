add_executable( s3-gtest s3_tests.cc
  ../src/AWSv4-impl.cc
  ../src/logging.cc
  ../src/S3AccessInfo.cc
  ../src/S3File.cc
  ../src/S3FileSystem.cc
  ../src/shortfile.cc
  ../src/stl_string_utils.cc
  ../src/HTTPCommands.cc 
  ../src/S3Commands.cc
)

add_executable( http-gtest http_tests.cc
  ../src/HTTPFile.cc
  ../src/HTTPFileSystem.cc
  ../src/HTTPCommands.cc
  ../src/stl_string_utils.cc
  ../src/shortfile.cc
  ../src/logging.cc
)


if( NOT XROOTD_PLUGINS_EXTERNAL_GTEST )
    add_dependencies(s3-gtest gtest)
    add_dependencies(http-gtest gtest)
    include_directories("${PROJECT_SOURCE_DIR}/vendor/gtest/googletest/include")
endif()

if(XROOTD_PLUGINS_EXTERNAL_GTEST)
    set(LIBGTEST "gtest")
else()
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
    ${CMAKE_CURRENT_BINARY_DIR}/http-gtest
)
