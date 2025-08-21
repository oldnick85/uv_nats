cmake_minimum_required(VERSION 3.25)
project(uv_nats LANGUAGES CXX)

# Настройка стандарта C++23
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# ==== SANITIZING ====
option(ENABLE_SANITIZERS "Enable sanitizers" OFF)

if(ENABLE_SANITIZERS)
    add_compile_options(
        -g
        -fno-omit-frame-pointer
        -fsanitize=address,undefined
        -fno-sanitize-recover=all
    )
    add_link_options(-fsanitize=address,undefined)
endif()

# Поиск зависимостей
find_package(cnats REQUIRED)
find_package(libuv REQUIRED)
find_package(argh REQUIRED)
find_package(spdlog REQUIRED)
find_package(nlohmann_json REQUIRED)

# Исполняемый файл
add_executable(${PROJECT_NAME} 
  src/main.cpp
  src/nats/nats_handler.cpp
  src/udp/udp_handler.cpp)

# Линковка библиотек
target_link_libraries(${PROJECT_NAME} 
    PRIVATE 
        cnats::nats_staticd
        uv
        argh
        spdlog::spdlog
        nlohmann_json::nlohmann_json
)
