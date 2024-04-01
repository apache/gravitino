# Gravitino Dev Env Image
This directory contains dockerfiles to build the docker image for the development environment.

A Gravitino development environment contains all necessary development tools installed as well as prebuilt Gravitino third
party dependencies.

## 1 Build dev env image

### 1.1 Build dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env.Dockerfile -t ghcr.io/OWNER/gravitino/dev-env:<tag> .
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env.Dockerfile -t ghcr.io/gravitino/dev-env:main .
```

## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/dev-env:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/dev-env:main
```

## 3 Build an image and run in local
```shell
./build-docker.sh
```
