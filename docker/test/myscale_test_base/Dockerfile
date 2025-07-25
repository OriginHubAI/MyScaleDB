# docker build -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-base:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-base:3.0.0 . --push
FROM  origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/runtime:3.0.0

ENV TZ=Asia/Shanghai
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone


# initial packages
RUN apt-get update \
    && apt-get install \
    fakeroot \
    ccache \
    --yes --no-install-recommends

# Sanitizer options for services (clickhouse-server)
# Set resident memory limit for TSAN to 45GiB (46080MiB) to avoid OOMs in Stress tests
# and MEMORY_LIMIT_EXCEEDED exceptions in Functional tests (total memory limit in Functional tests is ~55.24 GiB).
# TSAN will flush shadow memory when reaching this limit.
# It may cause false-negatives, but it's better than OOM.
#  max_allocation_size_mb is set to 32GB, so we have much bigger chance to run into memory limit than the limitation of the sanitizers
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "UBSAN_OPTIONS='print_stacktrace=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "ASAN_OPTIONS='halt_on_error=1 abort_on_error=1'" >> /etc/environment
# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1 max_allocation_size_mb=32768'
ENV UBSAN_OPTIONS='print_stacktrace=1 max_allocation_size_mb=32768'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768'
ENV LSAN_OPTIONS='max_allocation_size_mb=32768'
ENV ASAN_OPTIONS='halt_on_error=1 abort_on_error=1'

# for external_symbolizer_path, and also ensure that llvm-symbolizer really
# exists (since you don't want to fallback to addr2line, it is very slow)
RUN test -f /usr/bin/llvm-symbolizer-${LLVM_VERSION}
RUN ln -s /usr/bin/llvm-symbolizer-${LLVM_VERSION} /usr/bin/llvm-symbolizer

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
    apt-get install --yes --no-install-recommends \
    clang-${LLVM_VERSION} \
    debhelper \
    devscripts \
    gdb \
    gperf \
    lcov \
    netbase \
    perl \
    ripgrep \
    llvm-${LLVM_VERSION} \
    moreutils \
    pigz \
    pv \
    brotli \
    lsof \
    lz4 \
    npm \
    nodejs \
    expect \
    telnet \
    ncdu \
    tree \
    wget \
    rustc \
    cargo \
    netcat-openbsd \
    file \
    openssl \
    protobuf-compiler \
    qemu-user-static \
    sudo \
    # golang version 1.13 on Ubuntu 20 is enough for tests
    golang \
    unixodbc \
    mysql-client \
    postgresql-client \
    sqlite3 \
    openjdk-11-jre-headless \
    awscli \
    zstd \
    locales

RUN pip install numpy scipy pandas Jinja2 -i https://pypi.tuna.tsinghua.edu.cn/simple/
COPY process_functional_tests_result.py /

# This script is used to setup realtime export of server logs from the CI into external ClickHouse cluster:
COPY setup_export_logs.sh /

CMD sleep 1
