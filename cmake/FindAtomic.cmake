
# Ideas come from
#
#  https://gitlab.kitware.com/cmake/cmake/-/issues/17834
#
# But applied to the use of libatomic instead of libstdc++fs.
# The need to do this was highlighted as part of:
#   https://github.com/PelicanPlatform/xrootd-s3-http/pull/81
# The original driving use case was 32-bit builds; if we
# decide to drop those, we can rip out the target

include(CheckSourceCompiles)

function( check_working_cxx_atomics varname )
  CHECK_SOURCE_COMPILES( CXX "
    #include <cstdlib>
    #include <atomic>
    #include <cstdint>

    int main() {
      std::atomic<uint8_t> a1;
      std::atomic<uint16_t> a2;
      std::atomic<uint32_t> a3;
      std::atomic<uint64_t> a4;
      return a1++ + a2++ + a3++ + a4++;
    }" ${varname}
  )
endfunction( check_working_cxx_atomics varname )


check_working_cxx_atomics( CXX_ATOMIC_NO_LINK_NEEDED )

set( _found FALSE )
if( CXX_ATOMIC_NO_LINK_NEEDED )
  set( _found TRUE )
else()
  check_library_exists(atomic __atomic_fetch_add_4 "" HAVE_LIBATOMIC)
  if (HAVE_LIBATOMIC)
    set( OLD_CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} )
    list( APPEND CMAKE_REQUIRED_LIBRARIES "atomic" )
    check_working_cxx_atomics( HAVE_CXX_ATOMICS_WITH_LIB )
    set( CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQUIRED_LIBRARIES} )
    set( HAVE_CXX_ATOMICS_WITH_LIB TRUE )
    set( _found TRUE )
  endif()
endif()

add_library( std::atomic INTERFACE IMPORTED )

if( HAVE_CXX_ATOMICS_WITH_LIB )
  set_property( TARGET std::atomic APPEND PROPERTY INTERFACE_LINK_LIBRARIES atomic )
endif()

set( Atomic_FOUND ${_found} CACHE BOOL "TRUE if we can run a program using std::atomic" FORCE )
if( Atomic_FIND_REQUIRED AND NOT Atomic_FOUND )
    message( FATAL_ERROR "Cannot run simple program using std::atomic" )
endif()
