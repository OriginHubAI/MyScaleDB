set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/croaring")

set(SRCS
    "${LIBRARY_DIR}/src/array_util.c"
    "${LIBRARY_DIR}/src/bitset.c"
    "${LIBRARY_DIR}/src/bitset_util.c"
    "${LIBRARY_DIR}/src/isadetection.c"
    "${LIBRARY_DIR}/src/memory.c"
    "${LIBRARY_DIR}/src/roaring.c"
    "${LIBRARY_DIR}/src/roaring_array.c"
    "${LIBRARY_DIR}/src/roaring_priority_queue.c"
    "${LIBRARY_DIR}/src/containers/array.c"
    "${LIBRARY_DIR}/src/containers/bitset.c"
    "${LIBRARY_DIR}/src/containers/containers.c"
    "${LIBRARY_DIR}/src/containers/convert.c"
    "${LIBRARY_DIR}/src/containers/mixed_andnot.c"
    "${LIBRARY_DIR}/src/containers/mixed_equal.c"
    "${LIBRARY_DIR}/src/containers/mixed_intersection.c"
    "${LIBRARY_DIR}/src/containers/mixed_negation.c"
    "${LIBRARY_DIR}/src/containers/mixed_subset.c"
    "${LIBRARY_DIR}/src/containers/mixed_union.c"
    "${LIBRARY_DIR}/src/containers/mixed_xor.c"
    "${LIBRARY_DIR}/src/containers/run.c")

add_library(_roaring ${SRCS})

target_include_directories(_roaring PRIVATE "${LIBRARY_DIR}/include/roaring")
target_include_directories(_roaring SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_roaring SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/cpp")

# We redirect malloc/free family of functions to different functions that will track memory in ClickHouse.
# Also note that we exploit implicit function declarations.
target_compile_definitions(_roaring PRIVATE
    -Dmalloc=clickhouse_malloc
    -Dcalloc=clickhouse_calloc
    -Drealloc=clickhouse_realloc
    -Dreallocarray=clickhouse_reallocarray
    -Dfree=clickhouse_free
    -Dposix_memalign=clickhouse_posix_memalign)

target_link_libraries(_roaring PUBLIC clickhouse_common_io)

add_library(ch_contrib::roaring ALIAS _roaring)