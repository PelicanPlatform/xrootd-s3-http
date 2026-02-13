# Findtinyxml2.cmake
# Find the tinyxml2 library
#
# This module defines the following variables:
#   tinyxml2_FOUND - True if tinyxml2 was found
#   tinyxml2_INCLUDE_DIRS - Include directories for tinyxml2
#   tinyxml2_LIBRARIES - Libraries to link against
#
# This module defines the following imported targets:
#   tinyxml2::tinyxml2 - The tinyxml2 library

# Check if the target already exists (e.g., from a config package)
if(TARGET tinyxml2::tinyxml2)
	set(tinyxml2_FOUND TRUE)
	return()
endif()

# First, try to find tinyxml2 using pkg-config
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
	pkg_check_modules(PC_TINYXML2 QUIET tinyxml2)
endif()

# Find the include directory
find_path(tinyxml2_INCLUDE_DIR
	NAMES tinyxml2.h
	HINTS ${PC_TINYXML2_INCLUDE_DIRS}
	PATH_SUFFIXES tinyxml2
)

# Find the library
find_library(tinyxml2_LIBRARY
	NAMES tinyxml2
	HINTS ${PC_TINYXML2_LIBRARY_DIRS}
)

# Handle the QUIETLY and REQUIRED arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(tinyxml2
	REQUIRED_VARS tinyxml2_LIBRARY tinyxml2_INCLUDE_DIR
	VERSION_VAR PC_TINYXML2_VERSION
)

if(tinyxml2_FOUND)
	set(tinyxml2_LIBRARIES ${tinyxml2_LIBRARY})
	set(tinyxml2_INCLUDE_DIRS ${tinyxml2_INCLUDE_DIR})

	# Create the imported target if it doesn't exist
	if(NOT TARGET tinyxml2::tinyxml2)
		add_library(tinyxml2::tinyxml2 UNKNOWN IMPORTED)
		set_target_properties(tinyxml2::tinyxml2 PROPERTIES
			IMPORTED_LOCATION "${tinyxml2_LIBRARY}"
			INTERFACE_INCLUDE_DIRECTORIES "${tinyxml2_INCLUDE_DIR}"
		)
	endif()
endif()

mark_as_advanced(tinyxml2_INCLUDE_DIR tinyxml2_LIBRARY)
