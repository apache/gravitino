---
title: "Publish Docker images"
slug: /publish-docker-images
keyword: docker
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino project provides a set of Docker images to facilitate the publishing, development,
and testing of the Gravitino project.
[Apache Docker Hub](https://hub.docker.com/u/apache) repository publishes the official Gravitino Docker images.

## Publish Docker images to Docker Hub

You can use GitHub actions to publish Docker images to the Docker Hub repository.

1. Open the [Docker publish link](https://github.com/apache/gravitino/actions/workflows/docker-image.yml)
1. Click the *Run workflow* button.
1. Select the branch you want to build:
   - If you select the *main* branch, you will publish a Docker image with the specified tag and the `latest` tag.
   - If you select a different branch, you will publish a Docker image with the specified tag.
1. Choose the image you want to build:
   - `apache/gravitino-ci:hive`
   - `apache/gravitino-ci:trino`
   Future plans include support for other data sources.
1. Input the *tag name* (e.g. `0.1.0`), then build and push the Docker image name.
   Currently, the Docker image name is in the following format:
   - `apache/gravitino-ci:{image-type}-0.1.0`:
     A Trino CI image where the *image-type* is one of "trino", "hive", "kerberos-hive", "doris", or "ranger".
   - `apache/gravitino-playground:{image-type}-0.1.0`:
     A playground image where the *image-type* can be one of "trino", "hive", or "ranger".
   - `apache/gravitino:0.1.0`:
     A Gravitino server image.
   - `apache/gravitino-iceberg-rest:0.1.0`:
     An Iceberg REST server image.
1. You must enter the correct *docker user name* and *publish docker token*
   before you can execute run `Publish Docker Image` workflow.
1. If you want to update the latest tag, check `Whether to update the latest tag`.
1. Wait for the workflow to complete.
   You can see a new Docker image shown in the [Apache Docker Hub](https://hub.docker.com/u/apache) repository.

![Publish Docker image](../assets/publish-docker-image.png)

## Resources

- [Gravitino Docker image details](./docker-image-details.md)

