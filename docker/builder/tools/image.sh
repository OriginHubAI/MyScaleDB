#!/usr/bin/env bash

# All of env init in gitlab ci

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 [LICENSE_IMAGE] [IMAGE_PLATFORM]"
    echo "  LICENSE_IMAGE: true or false, default is false"
    echo "  IMAGE_PLATFORM: linux/amd64,linux/arm64, default is linux/amd64,linux/arm64"
    exit 1
fi

LICENSE_IMAGE=${1:-"false"}
IMAGE_PLATFORM=${2:-"linux/amd64,linux/arm64"}

source docker/builder/tools/version.sh

IMAGE_TAG=${INTERNAL_DOCKER_HUB_URL}/${INTERNAL_DOCKER_HUB_REPO}/myscaledb:${MYSCALE_VERSION_STRING}

cp -rfv artifacts/clickhouse-*.tgz docker/myscaledb/

# if need push image to other registry, add other tag and push it,
# will abandon other image shall script, please only expends this script
docker buildx build \
    --build-arg http_proxy=${HTTP_PROXY} \
    --build-arg https_proxy=${HTTP_PROXY} \
    --build-arg no_proxy=${NO_PROXY_RULES} \
    --platform ${IMAGE_PLATFORM} --build-arg version="${VERSION_STRING}" \
    --rm=true -t ${IMAGE_TAG} docker/myscaledb --push

rm -rfv docker/myscaledb/clickhouse-*.tgz
