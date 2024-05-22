find_package(PkgConfig REQUIRED)

pkg_check_modules(PC_aegis IMPORTED_TARGET QUIET aegis)

if(PC_aegis_FOUND)
    if(NOT TARGET aegis::aegis)
        set_target_properties(PkgConfig::PC_aegis PROPERTIES IMPORTED_GLOBAL TRUE)
        add_library(aegis::aegis ALIAS PkgConfig::PC_aegis)
    endif()
endif()