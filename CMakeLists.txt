cmake_minimum_required( VERSION 3.14 )

project( xrootd-http/s3 )

option( XROOTD_PLUGINS_EXTERNAL_GTEST "Use an external/pre-installed copy of GTest" OFF )
option( VALGRIND "Run select unit tests under valgrind" OFF )
option( ASAN "Build the plugin with the address sanitizer" OFF )

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake )
if( "${CMAKE_BUILD_TYPE}" STREQUAL "" )
  set( CMAKE_BUILD_TYPE Debug )
endif()

find_package( XRootD REQUIRED COMPONENTS UTILS SERVER )
find_package( CURL REQUIRED )
find_package( Threads REQUIRED )
find_package( OpenSSL REQUIRED )

if(VALGRIND)
  find_program(VALGRIND_BIN valgrind REQUIRED)
endif()

if(ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")
endif()

set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED ON )

if( CMAKE_BUILD_TYPE STREQUAL Debug )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror" )
endif()

if(NOT APPLE)
  SET( CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
  SET( CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,--no-undefined")
endif()

# Our custom Filesystem module creates the std::filesystem library target for
# any special dependencies needed for using std::filesystem.
find_package( Filesystem REQUIRED )
# Similar setup for libatomic; required only on 32-bit systems
find_package( Atomic REQUIRED )

if( NOT XROOTD_EXTERNAL_TINYXML2 )
  include(FetchContent)
  # Allow a locally-found tinyxml2 tarball to be used; provides the ability for packagers
  # to build this without any external network connectivity (as in the case of Koji).
  set( TINYXML2_URL "${CMAKE_CURRENT_SOURCE_DIR}/tinyxml2-10.0.0.tar.gz" )
  if( NOT EXISTS "${TINYXML2_URL}" )
    set( TINYXML2_URL "https://github.com/leethomason/tinyxml2/archive/refs/tags/10.0.0.tar.gz" )
  endif()
  cmake_policy( SET CMP0135 NEW )
  FetchContent_Declare(
    tinyxml2
    URL "${TINYXML2_URL}"
    URL_HASH SHA256=3bdf15128ba16686e69bce256cc468e76c7b94ff2c7f391cc5ec09e40bff3839
    )
  set(CMAKE_POSITION_INDEPENDENT_CODE ON CACHE INTERNAL "Force tinyxml2 to use PIC")
  FetchContent_MakeAvailable( tinyxml2 )
else()
  find_package( tinyxml2 REQUIRED )
endif()

##
# Flag is needed explicitly for 32-bit platforms but not currently exported by XRootD
# as a reverse dependency.  Can be removed once this is merged and in all supported
# releases:
#
# https://github.com/xrootd/xrootd/pull/2369
#
add_definitions( -D_FILE_OFFSET_BITS=64 )

######################
##  libXrdOssS3     ##
######################
# On Linux, we hide all the symbols for the final libraries, exposing only what's needed for the XRootD
# runtime loader.  So here we create the object library and will create a separate one for testing with
# the symbols exposed.
add_library(XrdS3Obj OBJECT src/CurlUtil.cc src/S3File.cc src/S3Directory.cc src/S3AccessInfo.cc src/S3FileSystem.cc src/AWSv4-impl.cc src/S3Commands.cc src/HTTPCommands.cc src/TokenFile.cc src/stl_string_utils.cc src/shortfile.cc src/logging.cc)
set_target_properties(XrdS3Obj PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(XrdS3Obj PRIVATE ${XRootD_INCLUDE_DIRS})
target_link_libraries( XrdS3Obj ${XRootD_UTILS_LIBRARIES} ${XRootD_SERVER_LIBRARIES} CURL::libcurl OpenSSL::Crypto tinyxml2::tinyxml2 Threads::Threads std::filesystem std::atomic )

# Compatability library, doesn't match upstream's naming convention
add_library(XrdS3 MODULE "$<TARGET_OBJECTS:XrdS3Obj>")
target_link_libraries(XrdS3 XrdS3Obj)
# New library name, matches upstream's naming convention
add_library(XrdOssS3 MODULE "$<TARGET_OBJECTS:XrdS3Obj>")
target_link_libraries(XrdOssS3 XrdS3Obj)

######################
##  libXrdOssHTTP   ##
######################
add_library(XrdHTTPServerObj OBJECT src/CurlUtil.cc src/HTTPFile.cc src/HTTPFileSystem.cc src/HTTPCommands.cc src/TokenFile.cc src/stl_string_utils.cc src/shortfile.cc src/logging.cc)
set_target_properties(XrdHTTPServerObj PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(XrdHTTPServerObj PRIVATE ${XRootD_INCLUDE_DIRS})
target_link_libraries(XrdHTTPServerObj ${XRootD_UTILS_LIBRARIES} ${XRootD_SERVER_LIBRARIES} CURL::libcurl OpenSSL::Crypto Threads::Threads std::filesystem)

add_library(XrdHTTPServer MODULE "$<TARGET_OBJECTS:XrdHTTPServerObj>")
target_link_libraries(XrdHTTPServer XrdHTTPServerObj)
# New library name, matches upstream's naming convention
add_library(XrdOssHttp MODULE "$<TARGET_OBJECTS:XrdHTTPServerObj>")
target_link_libraries(XrdOssHttp XrdHTTPServerObj)

######################
## libXrdOssFilter  ##
######################
add_library( XrdOssFilterObj OBJECT src/Filter.cc src/logging.cc )
set_target_properties( XrdOssFilterObj PROPERTIES POSITION_INDEPENDENT_CODE ON )
target_include_directories( XrdOssFilterObj PRIVATE ${XRootD_INCLUDE_DIRS} )
target_link_libraries( XrdOssFilterObj ${XRootD_UTILS_LIBRARIES} ${XRootD_SERVER_LIBRARIES} )

add_library( XrdOssFilter MODULE "$<TARGET_OBJECTS:XrdOssFilterObj>" )
target_link_libraries( XrdOssFilter XrdOssFilterObj )

# Customize module's suffix and, on Linux, hide unnecessary symbols
if( APPLE )
  set_target_properties( XrdS3 PROPERTIES OUTPUT_NAME "XrdS3-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" )
  set_target_properties( XrdHTTPServer PROPERTIES OUTPUT_NAME "XrdHTTPServer-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" )
  set_target_properties( XrdOssS3 PROPERTIES OUTPUT_NAME "XrdOssS3-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" )
  set_target_properties( XrdOssHttp PROPERTIES OUTPUT_NAME "XrdOssHttp-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" )
  set_target_properties( XrdOssFilter PROPERTIES OUTPUT_NAME "XrdOssFilter-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" )
else()
  set_target_properties( XrdS3 PROPERTIES OUTPUT_NAME "XrdS3-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols" )
  set_target_properties( XrdHTTPServer PROPERTIES OUTPUT_NAME "XrdHTTPServer-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols" )
  set_target_properties( XrdOssS3 PROPERTIES OUTPUT_NAME "XrdOssS3-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols" )
  set_target_properties( XrdOssHttp PROPERTIES OUTPUT_NAME "XrdOssHttp-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols" )
  set_target_properties( XrdOssFilter PROPERTIES OUTPUT_NAME "XrdOssFilter-${XRootD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols" )
endif()

include(GNUInstallDirs)

install(
  TARGETS XrdS3 XrdHTTPServer XrdOssS3 XrdOssHttp XrdOssFilter
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

if( BUILD_TESTING )
  # Create shared libraries for testing from the existing objects
  add_library(XrdS3Testing SHARED "$<TARGET_OBJECTS:XrdS3Obj>")
  target_link_libraries(XrdS3Testing XrdS3Obj)
  target_include_directories(XrdS3Testing INTERFACE ${XRootD_INCLUDE_DIRS})

  add_library(XrdHTTPServerTesting SHARED "$<TARGET_OBJECTS:XrdHTTPServerObj>")
  target_link_libraries(XrdHTTPServerTesting XrdHTTPServerObj)
  target_include_directories(XrdHTTPServerTesting INTERFACE ${XRootD_INCLUDE_DIRS})

  add_library( XrdOssFilterTesting SHARED "$<TARGET_OBJECTS:XrdOssFilterObj>" )
  target_link_libraries( XrdOssFilterTesting XrdOssFilterObj )
  target_include_directories( XrdOssFilterTesting INTERFACE ${XRootD_INCLUDE_DIRS} )

  find_program(GoWrk go-wrk HINTS "$ENV{HOME}/go/bin")
  if( NOT GoWrk )
    # Try installing the go-wrk variable to generate a reasonable stress test
    execute_process( COMMAND go install github.com/bbockelm/go-wrk@92dbe19
      RESULT_VARIABLE go_install_result )
    if( go_install_result EQUAL 0 )
      find_program(GoWrk go-wrk HINTS "$ENV{HOME}/go/bin")
    else()
      message(ERROR "Failed to install the go-wrk binary" )
    endif()
  endif()

  if( NOT XROOTD_PLUGINS_EXTERNAL_GTEST )
    include( FetchContent )
    set( GTEST_URL "${CMAKE_CURRENT_SOURCE_DIR}/googletest-1.15.2.tar.gz" )
    if( NOT EXISTS "${GTEST_URL}" )
        set( GTEST_URL "https://github.com/google/googletest/releases/download/v1.15.2/googletest-1.15.2.tar.gz" )
    endif()
    cmake_policy(SET CMP0135 NEW)
    FetchContent_Declare(GTest
      URL "${GTEST_URL}"
      URL_HASH SHA256=7b42b4d6ed48810c5362c265a17faebe90dc2373c885e5216439d37927f02926
      TEST_COMMAND ""
    )
    FetchContent_MakeAvailable( GTest )
  else()
    find_package(GTest REQUIRED)
  endif()

  enable_testing()
  add_subdirectory(test)
endif()
