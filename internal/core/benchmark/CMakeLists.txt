# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(fastmem_benchmark FastMemBenchmark.cpp)
target_include_directories(fastmem_benchmark PRIVATE ${CMAKE_HOME_DIRECTORY}/src)
target_link_libraries(fastmem_benchmark PRIVATE benchmark::benchmark)
install(TARGETS fastmem_benchmark DESTINATION benchmark)

add_executable(raw_column_access_benchmark RawColumnAccessBenchmark.cpp)
target_include_directories(raw_column_access_benchmark PRIVATE
        ${CMAKE_HOME_DIRECTORY}/src
        ${CMAKE_HOME_DIRECTORY}/unittest
        ${CMAKE_HOME_DIRECTORY}/output/include
        ${CMAKE_HOME_DIRECTORY}/thirdparty/tantivy
        ${CMAKE_HOME_DIRECTORY}/thirdparty/tantivy/tantivy-binding/include
        ${MILVUS_STORAGE_INCLUDE_DIR}
)
target_compile_definitions(raw_column_access_benchmark PRIVATE MILVUS_UNIT_TEST)
target_link_directories(raw_column_access_benchmark PRIVATE
        ${CMAKE_HOME_DIRECTORY}/output/lib
)
target_link_libraries(raw_column_access_benchmark PRIVATE
        benchmark::benchmark
        GTest::gtest
        milvus_core
        milvus_conan_deps
        knowhere
        milvus-storage
        milvus-planparser-cpp
)
if (LINUX)
    target_link_options(raw_column_access_benchmark PRIVATE
            "LINKER:--allow-multiple-definition")
endif()
set_target_properties(raw_column_access_benchmark PROPERTIES
        BUILD_RPATH "${CMAKE_HOME_DIRECTORY}/output/lib"
)
