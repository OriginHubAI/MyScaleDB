# docker build -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-stress:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-stress:3.0.0 . --push
FROM origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-stateless:3.0.0

RUN apt-get update --allow-releaseinfo-change -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            parallel \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            sudo \
            openssl \
            netcat-openbsd \
            telnet \
            brotli \
    && apt-get clean

ENV EXPORT_S3_STORAGE_POLICIES=1

SHELL ["/bin/bash", "-c"]
CMD bash
