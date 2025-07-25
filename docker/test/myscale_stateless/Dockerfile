# docker build -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-stateless:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-stateless:3.0.0 . --push
FROM origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-base:3.0.0

ARG TARGETARCH
# ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.6.20200320/clickhouse-odbc-1.1.6-Linux.tar.gz"

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        zip \
        unzip \
        p7zip-full \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ENV TZ=Asia/Shanghai
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

ENV NUM_TRIES=1

# Unrelated to vars in setup_minio.sh, but should be the same there
# to have the same binaries for local running scenario
ARG MINIO_SERVER_VERSION=2024-08-03T04-33-23Z
ARG MINIO_CLIENT_VERSION=2024-07-31T15-58-33Z
ARG TARGETARCH

# Download Protoc
ARG PROTOC_VERSION=25.1
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local \
    && rm protoc-${PROTOC_VERSION}-linux-x86_64.zip

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

# RUN mkdir -p /tmp/clickhouse-odbc-tmp \
#   && cd /tmp/clickhouse-odbc-tmp \
#   && curl -L ${odbc_driver_url} | tar --strip-components=1 -xz clickhouse-odbc-1.1.6-Linux \
#   && mkdir /usr/local/lib64 -p \
#   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib64/ \
#   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
#   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
#   && sed -i 's"=libclickhouseodbc"=/usr/local/lib64/libclickhouseodbc"' /etc/odbcinst.ini \
#   && rm -rf /tmp/clickhouse-odbc-tmp

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && curl -L "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -o ./minio \
    && curl -L "https://dl.min.io/client/mc/release/linux-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -o ./mc \
    && chmod +x ./mc ./minio

# Download hadoop 
RUN curl -L --no-verbose -O 'https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz' \
    && tar -xvf hadoop-3.3.1.tar.gz \
    && rm -rf hadoop-3.3.1.tar.gz

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"
ENV EXPORT_S3_STORAGE_POLICIES=1
ENV CLICKHOUSE_GRPC_CLIENT="/usr/share/clickhouse-utils/grpc-client/clickhouse-grpc-client.py"

RUN npm install -g azurite@3.30.0 \
    && npm install -g tslib && npm install -g node

COPY setup_minio.sh /
COPY setup_hdfs_minicluster.sh /
COPY attach_gdb.lib /
COPY utils.lib /

# We store stress_tests.lib in stateless image to avoid duplication of this file in stress and upgrade tests
COPY stress_tests.lib /
