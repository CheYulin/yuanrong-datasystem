add_library(common_device_hbm_ipc STATIC
    mock_ipc_hbm_backend.cpp
)

target_include_directories(common_device_hbm_ipc PUBLIC
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(common_device_hbm_ipc PUBLIC common_util)
