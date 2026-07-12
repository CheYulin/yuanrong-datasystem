add_library(common_device_nds STATIC
    alignment_gate.cpp
    fake_nds_spill_reader.cpp
)

target_include_directories(common_device_nds PUBLIC
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(common_device_nds PUBLIC common_util)
