# docker build -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-sqltest:3.0.0 .
# docker buildx build --platform linux/amd64,linux/arm64 -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-sqltest:3.0.0 . --push
FROM origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-base:3.0.0

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

ARG sqltest_repo="https://github.com/elliotchance/sqltest/"

RUN git clone ${sqltest_repo}

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

SHELL ["/bin/bash", "-c"]
CMD bash
