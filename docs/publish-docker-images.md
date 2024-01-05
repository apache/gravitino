---
title: "Publish Docker images"
slug: /publish-docker-images
keyword: docker
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---


## Introduction

The Gravitino project provides a set of Docker images to facilitate the publishing, development, and testing of the Gravitino project.
[Datastrato Docker Hub](https://hub.docker.com/u/datastrato) repository publishes the official Gravitino Docker images.

## Publish Docker images to Docker Hub

You can use GitHub actions to publish Docker images to the Docker Hub repository.

1. Open the [Docker publish link](https://github.com/datastrato/gravitino/actions/workflows/docker-image.yml)
2. Click the `Run workflow` button.
3. Select the branch you want to build
   + Selecting the main branch results in publishing the Docker image with the specified tag and the latest tag.
   + Selecting another branch, results are publishing the Docker image with the specified tag.
4. Choose the image you want to build
   + `datastrato/gravitino-ci-hive`.
   + `datastrato/gravitino-ci-trino`.
   + Future plans include support for other data sources.
5. Input the `tag name`, for example: `0.1.0`, Then build and push the Docker image name as `datastrato/{image-name}:0.1.0`.
6. You must enter the correct `publish docker token` before you can execute run `Publish Docker Image` workflow.
7. Wait for the workflow to complete. You can see a new Docker image shown in the [Datastrato Docker Hub](https://hub.docker.com/u/datastrato) repository.

![Publish Docker image](assets/publish-docker-image.png)

## More details of Gravitino Docker images

+ [Gravitino Docker images](docker-image-details.md)
