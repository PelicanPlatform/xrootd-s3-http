cmake_minimum_required( VERSION 3.13 )

project( xrootd-s3 )

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake )

find_package( Xrootd REQUIRED )
find_package( CURL REQUIRED )

include (FindPkgConfig)
pkg_check_modules(LIBCRYPTO REQUIRED libcrypto)

if(NOT XROOTD_PLUGIN_VERSION)
  exec_program(${XROOTD_BIN}/xrootd-config ARGS "--plugin-version" OUTPUT_VARIABLE XROOTD_PLUGIN_VERSION RETURN_VALUE RETVAR)
  set(XROOTD_PLUGIN_VERSION ${XROOTD_PLUGIN_VERSION} CACHE INTERNAL "")
endif()

macro(use_cxx11)
  if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      set (CMAKE_CXX_FLAGS "-std=gnu++11 ${CMAKE_CXX_FLAGS}")
    endif ()
  else ()
    set (CMAKE_CXX_STANDARD 11)
  endif ()
endmacro(use_cxx11)
use_cxx11()

if( CMAKE_COMPILER_IS_GNUCXX )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror" )
endif()
SET( CMAKE_SHARED_LINKER_FLAGS "-Wl,--no-undefined")
SET( CMAKE_MODULE_LINKER_FLAGS "-Wl,--no-undefined")

include_directories(${XROOTD_INCLUDES} ${CURL_INCLUDE_DIRS} ${LIBCRYPTO_INCLUDE_DIRS})

add_library(XrdS3 SHARED src/s3.cpp src/S3FileSystem.cc src/AWSUtils.cc)
target_link_libraries(XrdS3 -ldl ${XROOTD_UTILS_LIB} ${XROOTD_SERVER_LIB} ${CURL_LIBRARIES} ${LIBCRYPTO_LIBRARIES})
set_target_properties(XrdS3 PROPERTIES OUTPUT_NAME "XrdS3-${XROOTD_PLUGIN_VERSION}" SUFFIX ".so" LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/configs/export-lib-symbols")

SET(LIB_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib" CACHE PATH "Install path for libraries")

install(
  TARGETS XrdS3
  LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

install(
  FILES ${CMAKE_SOURCE_DIR}/configs/60-s3.cfg
  DESTINATION ${CMAKE_INSTALL_PREFIX}/etc/xrootd/config.d/
)
