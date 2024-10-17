#!/usr/bin/env bash
set -e

docker build --rm=true -t origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/runtime:3.0.0 .
