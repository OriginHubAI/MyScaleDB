FROM docker.m.daocloud.io/ubuntu:22.04 as builder

ARG TARGETARCH
ARG version=24.8.8.1

RUN mkdir -pv /builder/mqdb

ADD clickhouse-${version}-${TARGETARCH:-amd64}.tgz /builder/mqdb/


FROM origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/runtime:3.0.1 as runtime

ARG TARGETARCH
ARG version=24.8.8.1

VOLUME ["/var/lib/clickhouse", "/etc/clickhouse-server", "/etc/clickhouse-client/"]
EXPOSE 9000 8123 9009

COPY --from=builder /builder/mqdb/clickhouse-${version}-${TARGETARCH:-amd64}/ /

ADD server.conf /etc/clickhouse-server
ADD client.conf /etc/clickhouse-client

RUN mkdir -pv /docker-entrypoint-initdb.d
ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]



