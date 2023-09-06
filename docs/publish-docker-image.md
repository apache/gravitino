<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# How to Publish Docker images

## Introduction
The Graviton project provides a set of docker images to facilitate the publish, development and testing of the Graviton project.
The Graviton official docker images are published to the [datastrato](https://hub.docker.com/u/datastrato) DockerHub repository.

## Publish Docker Images to Docker Hub Flow

We use Github Actions to publish the docker images to the DockerHub repository.
1. Open the https://github.com/datastrato/graviton/actions/workflows/docker-image.yml
2. Click the `Run workflow` button.
3. Select the branch you want to publish.

   + If you select `main` branch, the docker image will be published same to specified tag and the `latest` tag.

   + If you select another branch, the docker image will only publish the specified tag.

4. Input the `tag name`, for example: `0.1.0`, Then build and push the docker image name is `datastrato/{image-name}:0.1.0`.

   + Currently, we only support publishing the `hive` docker image, for example: `datastrato/graviton-ci-hive:0.1.0`.
   + We will support publishing other data sources, for example, `Iecberg` docker image in the future.

5. You must enter the correct `publish docker token` before you can execute run `Publish Docker Image` workflow.
6. Wait for the workflow to complete. You can see a new docker image shown in the [datastrato](https://hub.docker.com/u/datastrato) DockerHub repository.

[<img src="assets/publish-docker-image.png" width="400"/>](assets/publish-docker-image.png)

## The version of the Data source in the Docker image
| Docker image name | Docker image tag | Data source version      |
|-------------------|------------------|--------------------------|
| graviton-ci-hive  | 0.1.0            | hadoop-2.7.3, hive-2.3.9 |
