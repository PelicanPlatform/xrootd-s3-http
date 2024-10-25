cmake_minimum_required( VERSION 3.13 )

project( xrootd-http/s3 )

option( XROOTD_PLUGINS_BUILD_UNITTESTS "Build the scitokens-cpp unit tests" OFF )
option( XROOTD_PLUGINS_EXTERNAL_GTEST "Use an external/pre-installed copy of GTest" OFF )

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake )
set( CMAKE_BUILD_TYPE Debug )

find_package( Xrootd REQUIRED )
find_package( CURL REQUIRED )
find_package( Threads REQUIRED )

include (FindPkgConfig)
pkg_check_modules(LIBCRYPTO REQUIRED libcrypto)

if(NOT XROOTD_PLUGIN_VERSION)
  exec_program(${XROOTD_BIN}/xrootd-config ARGS "--plugin-version" OUTPUT_VARIABLE XROOTD_PLUGIN_VERSION RETURN_VALUE RETVAR)
  set(XROOTD_PLUGIN_VERSION ${XROOTD_PLUGIN_VERSION} CACHE INTERNAL "")
endif()

macro(use_cxx17)
  if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      set (CMAKE_CXX_FLAGS "-std=c++17 ${CMAKE_CXX_FLAGS}")
    endif ()
  else ()
    set (CMAKE_CXX_STANDARD 17)
  endif ()

  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 8.0)
      if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
        link_libraries(stdc++fs)
      endif()
    endif()
  endif()
endmacro(use_cxx17)
use_cxx17()

if( CMAKE_COMPILER_IS_GNUCXX )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror" )
endif()

if(NOT APPLE)
  SET( CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
  SET( CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -Wl,--no-undefined")
endif()

if( NOT XROOTD_EXTERNAL_TINYXML2 )
	set(CMAKE_POSITION_INDEPENDENT_CODE ON)
	add_subdirectory(vendor/tinyxml2)
else()
	find_package(tinyxml2)
endif()

include_directories(${XROOTD_INCLUDES} ${CURL_INCLUDE_DIRS} ${LIBCRYPTO_INCLUDE_DIRS})

add_library(XrdS3 SHARED src/CurlUtil.cc src/S3File.cc src/S3Directory.cc src/S3AccessInfo.cc src/S3FileSystem.cc src/AWSv4-impl.cc src/S3Commands.cc src/HTTPCommands.cc src/TokenFile.cc src/stl_string_utils.cc src/shortfile.cc src/logging.cc)
add_library(XrdHTTPServer SHARED src/CurlUtil.cc src/HTTPFile.cc src/HTTPFileSystem.cc src/HTTPCommands.cc src/TokenFile.cc src/stl_string_utils.cc src/shortfile.cc src/logging.cc)

target_link_libraries(XrdS3 -ldl ${XROOTD_UTILS_LIB} ${XROOTD_SERVER_LIB} ${CURL_LIBRARIES} ${LIBCRYPTO_LIBRARIES} tinyxml2::tinyxml2 Threads::Threads)
target_link_libraries(XrdHTTPServer -ldl ${XROOTD_UTILS_LIB} ${XROOTD_SERVER_LIB} ${CURL_LIBRARIES} ${LIBCRYPTO_LIBRARIES} Threads::Threads)

# The CMake documentation strongly advises against using these macros; instead, the pkg_check_modules
# is supposed to fill out the full path to ${LIBCRYPTO_LIBRARIES}.  As of cmake 3.26.1, this does not
# appear to be the case on Mac OS X.  Remove once no longer necessary!
target_link_directories(XrdS3 PUBLIC ${LIBCRYPTO_LIBRARY_DIRS})
target_link_directories(XrdHTTPServer PUBLIC ${LIBCRYPTO_LIBRARY_DIRS})

if(NOT APPLE)
  set_target_properties(XrdS3 PROPERTIES OUTPUT_NAME "XrdS3-${XROOTD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols")
  set_target_properties(XrdHTTPServer PROPERTIES OUTPUT_NAME "XrdHTTPServer-${XROOTD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols")
else()
  set_target_properties(XrdS3 PROPERTIES OUTPUT_NAME "XrdS3-${XROOTD_PLUGIN_VERSION}" SUFFIX ".so")
  set_target_properties(XrdHTTPServer PROPERTIES OUTPUT_NAME "XrdHTTPServer-${XROOTD_PLUGIN_VERSION}" SUFFIX ".so")
endif()

SET(LIB_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib" CACHE PATH "Install path for libraries")

install(
  TARGETS XrdS3 XrdHTTPServer
  LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

if( XROOTD_PLUGINS_BUILD_UNITTESTS )
  if( NOT XROOTD_PLUGINS_EXTERNAL_GTEST )
    include(ExternalProject)
    ExternalProject_Add(gtest
      PREFIX external/gtest
      URL ${CMAKE_CURRENT_SOURCE_DIR}/vendor/gtest
      BUILD_BYPRODUCTS external/gtest/src/gtest-build/lib/libgtest.a
      INSTALL_COMMAND :
    )
  endif()
  enable_testing()
  add_subdirectory(test)
endif()

#install(
#  FILES ${CMAKE_SOURCE_DIR}/configs/60-s3.cfg
#  DESTINATION ${CMAKE_INSTALL_PREFIX}/etc/xrootd/config.d/
#)
