cmake_minimum_required(VERSION 3.14)
project(livox_bag LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(livox_msg REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(std_msgs REQUIRED)

fins_add_node(${PROJECT_NAME} SHARED 
  src/nodes.cpp
)

target_link_libraries(${PROJECT_NAME} PUBLIC
  livox_msg::livox_msg
)

fins_link_ros_dependencies(${PROJECT_NAME}
  std_msgs
  sensor_msgs
)
