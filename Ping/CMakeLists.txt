add_executable(
        client
        client.cpp
)

target_link_libraries(
    client
    ${LIBFABRIC_LIBRARIES}
)

add_executable(
        server
        server.cpp
)

target_link_libraries(
    server
    ${LIBFABRIC_LIBRARIES}
)