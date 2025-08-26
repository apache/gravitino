---
title: "Docker image details"
slug: /docker-image-details
keyword: docker
license: "This software is licensed under the Apache License version 2."
---

# User Docker images

## Apache Gravitino Docker image

You can deploy the service with the Gravitino Docker image.

Container startup commands

```shell
docker run --rm -d -p 8090:8090 -p 9001:9001 apache/gravitino:0.7.0-incubating
```

Changelog


- apache/gravitino:0.9.1
  - Based on Gravitino 0.9.1, you can know more information from 0.9.1 [release notes](https://github.com/apache/gravitino/releases/tag/v0.9.1).

- apache/gravitino:0.9.0-incubating
  - Based on Gravitino 0.9.0-incubating, you can know more information from 0.9.0-incubating [release notes](https://github.com/apache/gravitino/releases/tag/v0.9.0-incubating).

- apache/gravitino:0.8.0-incubating
  - Based on Gravitino 0.8.0-incubating, you can know more information from 0.8.0-incubating [release notes](https://github.com/apache/gravitino/releases/tag/v0.8.0-incubating).

- apache/gravitino:0.7.0-incubating
  - Based on Gravitino 0.7.0-incubating, you can know more information from 0.7.0-incubating [release notes](https://github.com/apache/gravitino/releases/tag/v0.7.0-incubating).
  - Place bundle jars (gravitino-aws-bundle.jar, gravitino-gcp-bundle.jar, gravitino-aliyun-bundle.jar) in the `${GRAVITINO_HOME}/catalogs/hadoop/libs` folder to support the cloud storage catalog without manually adding the jars to the classpath.

- apache/gravitino:0.6.1-incubating
  - Based on Gravitino 0.6.1-incubating, you can know more information from 0.6.1-incubating release notes.

- apache/gravitino:0.6.0-incubating (Switch to Apache official DockerHub repository)
  - Use the latest Gravitino version 0.6.0-incubating source code to build the image.

- datastrato/gravitino:0.5.1
  - Based on Gravitino 0.5.1, you can know more information from 0.5.1 release notes.

- datastrato/gravitino:0.5.0
  - Based on Gravitino 0.5.0, you can know more information from 0.5.0 release notes.

- datastrato/gravitino:0.4.0
  - Based on Gravitino 0.4.0, you can know more information from 0.4.0 release notes.

- datastrato/gravitino:0.3.1
  - Fix some issues

- datastrato/gravitino:0.3.0
  - Docker image `datastrato/gravitino:0.3.0`
  - Gravitino Server
  - Expose ports:
    - `8090` Gravitino Web UI
    - `9001` Iceberg REST service

## Apache Gravitino Iceberg REST server Docker image

You can deploy the standalone Gravitino Iceberg REST server with the Docker image.

Container startup commands

```shell
docker run --rm -d -p 9001:9001 apache/gravitino-iceberg-rest:0.7.0-incubating
```

Changelog
- apache/gravitino-iceberg-rest:0.9.1
  - Fix the issue that Iceberg REST server fail to start when enabling OAuth.
  - Add the documents for the StarRocks and Apache Doris using IRC

- apache/gravitino-iceberg-rest:0.9.0-incubating
  - Upgrade Iceberg version from 1.5 to 1.6.
  - Supports s3 path-style-access property.
  - Bug fix for warehouse is hard code in Iceberg memory catalog backend.


- apache/gravitino-iceberg-rest:0.8.0-incubating
  - Supports OSS and ADLS storage.
  - Supports event listener.
  - Supports audit log.

- apache/gravitino-iceberg-rest:0.7.0-incubating
  - Using JDBC catalog backend.
  - Supports S3 and GCS storage.
  - Supports credential vending.
  - Supports changing configuration by environment variables.

- apache/gravitino-iceberg-rest:0.6.1-incubating
  - Based on Gravitino 0.6.1-incubating, you can know more information from 0.6.1-incubating release notes.

- apache/gravitino-iceberg-rest:0.6.0-incubating.
  - Gravitino Iceberg REST Server with memory catalog backend.
  - Expose ports:
    - `9001` Iceberg REST service

## Apache Gravitino MCP server image

You can deploy the Gravitino MCP server with the Docker image.

Container startup commands

```shell
docker run --rm -d -p 8000:8000 apache/gravitino-mcp-server:latest --metalake test --transport http --mcp-url http://0.0.0.0:8000/mcp
```

Changelog
- apache/gravitino-mcp-server:1.0.0
  - Supports read operations for `catalog`, `schema`, `table`, `fileset`, `model`, `policy`, `topic`, `statistic`, `job`.
  - Supports associate&disassociate tag, policy to metadata
  - Supports submit&cancel jobs.

## Playground Docker image

You can use the [playground](https://github.com/apache/gravitino-playground) to experience the whole Gravitino system with other components.

The playground consists of multiple Docker images.

The Docker images of the playground have suitable configurations for users to experience.

### Apache Hive image

Changelog

- apache/gravitino-playground:hive-2.7.3 (Switch to Apache official DockerHub repository)
  - Use `datastrato/hive:2.7.3-no-yarn` Dockerfile to rebuild the image.

- datastrato/hive:2.7.3-no-yarn
  - Docker image `datastrato/hive:2.7.3-no-yarn`
  - `hadoop-2.7.3`
  - `hive-2.3.9`
  - Don't start YARN when container startup

### Trino image

Changelog


- apache/gravitino-playground:trino-435-gravitino-0.9.1
  - Use Gravitino release 0.9.1 Dockerfile to build the image.

- apache/gravitino-playground:trino-435-gravitino-0.9.0-incubating
  - Use Gravitino release 0.9.0-incubating Dockerfile to build the image.

- apache/gravitino-playground:trino-435-gravitino-0.8.0-incubating
  - Use Gravitino release 0.8.0-incubating Dockerfile to build the image.

- apache/gravitino-playground:trino-435-gravitino-0.7.0-incubating
  - Use Gravitino release 0.7.0-incubating Dockerfile to build the image.

- apache/gravitino-playground:trino-435-gravitino-0.6.1-incubating
  - Use Gravitino release 0.6.1-incubating Dockerfile to build the image.

- apache/gravitino-playground:trino-435-gravitino-0.6.0-incubating (Switch to Apache official DockerHub repository)
  - Use Gravitino release 0.6.0 Dockerfile to build the image.

- datastrato/trino:435-gravitino-0.5.1
  - Based on Gravitino 0.5.1, you can know more information from 0.5.1 release notes.

- datastrato/trino:426-gravitino-0.5.0
  - Based on Gravitino 0.5.0, you can know more information from 0.5.0 release notes.

- datastrato/trino:426-gravitino-0.4.0
  - Based on Gravitino 0.4.0, you can know more information from 0.4.0 release notes.

- datastrato/trino:426-gravitino-0.3.1
  - Fix some issues

- datastrato/trino:426-gravitino-0.3.0
  - Docker image `datastrato/trino:426-gravitino-0.3.0`
  - Base on `trino:426`
  - Added Gravitino trino-connector-0.3.0 libraries into the `/usr/lib/trino/plugin/gravitino`

# Developer Docker images

You can use these kinds of Docker images to facilitate integration testing of all catalog and connector modules within Gravitino.

## Apache Gravitino CI Apache Hive image with kerberos enabled

You can use this kind of image to test the catalog of Apache Hive with kerberos enable

Changelog

- apache/gravitino-ci:kerberos-hive-0.1.6
  - Change username from `datastrato` to `gravitino`.
    For more information, see [PR](https://github.com/apache/gravitino/pull/7040)

- apache/gravitino-ci:kerberos-hive-0.1.5 (Switch to Apache official DockerHub repository)
  - Use Gravitino release 0.6.0 Dockerfile to build the image.

- datastrato/gravitino-ci-kerberos-hive:0.1.5
  - Start another HMS for the Hive cluster in the container with port 19083. This is to test whether Kerberos authentication works for a Kerberos-enabled Hive cluster with multiple HMS.
  - Refresh ssh keys in the startup script.
  - Add test logic to log in localhost via ssh without password.

- datastrato/gravitino-ci-kerberos-hive:0.1.4
  - Increase the total check time for the status of DataNode to 150s.
  - Output the log of the DataNode fails to start

- datastrato/gravitino-ci-kerberos-hive:0.1.3
  - Add more proxy users in the core-site.xml file.
  - fix bugs in the `start.sh` script.

- datastrato/gravitino-ci-kerberos-hive:0.1.2
  - Add `${HOSTNAME} >> /root/.ssh/known_hosts` to the startup script.
  - Add check for the status of DataNode, if the DataNode is not running or ready within 100s, the container will exit.

- datastrato/gravitino-ci-kerberos-hive:0.1.1
  - Add a principal for Gravitino web server named 'HTTP/localhost@HADOOPKRB'.
  - Fix bugs about the configuration of proxy users. 

- datastrato/gravitino-ci-kerberos-hive:0.1.0
    - Set up a Hive cluster with kerberos enabled.
    - Install a KDC server and create a principal for Hive. For more please see [kerberos-hive](../dev/docker/kerberos-hive)

## Apache Gravitino CI Apache Hive image

You can use this kind of image to test the catalog of Apache Hive.

Changelog

- apache/gravitino-ci:hive-0.1.20
  - Change username from `datastrato` to `gravitino`.
    For more information, see [PR](https://github.com/apache/gravitino/pull/7040)

- apache/gravitino-ci:hive-0.1.19
  - Build ranger packages from source.
 
- apache/gravitino-ci:hive-0.1.18
  - Support UTF-8 encoding for the `hive-site.xml` file and Hive Metastore. 
    For more information, please see [PR](https://github.com/apache/gravitino/pull/6625)
  - Change ranger-hive-plugin and ranger-hdfs-plugin download URL. 

- apache/gravitino-ci:hive-0.1.17
  - Add support for JDBC SQL standard authorization
    - Add JDBC SQL standard authorization related configuration in the `hive-site-for-sql-base-auth.xml` and `hiveserver2-site-for-sql-base-auth.xml`
- 
- apache/gravitino-ci:hive-0.1.16
  - Add GCS related configuration in the `hive-site.xml` file.
  - Add GCS bundle jar in the `${HADOOP_HOME}/share/hadoop/common/lib/`

- apache/gravitino-ci:hive-0.1.15
  - Add Azure Blob Storage(ADLS) related configurations in the `hive-site.xml` file.

- apache/gravitino-ci:hive-0.1.14 
  - Add amazon S3 related configurations in the `hive-site.xml` file.
    - `fs.s3a.access.key` The access key for the S3 bucket.
    - `fs.s3a.secret.key` The secret key for the S3 bucket.
    - `fs.s3a.endpoint` The endpoint for the S3 bucket.

- apache/gravitino-ci:hive-0.1.13 (Switch to Apache official DockerHub repository)
  - Use Gravitino release 0.6.0 Dockerfile to build the image.

- datastrato/gravitino-ci-hive:0.1.13
  - Support Hive 2.3.9 and HDFS 2.7.3
    - Docker environment variables:
      - `HIVE_RUNTIME_VERSION`: `hive2` (default)
  - Support Hive 3.1.3, HDFS 3.1.0 and Ranger plugin version 2.4.0
    - Docker environment variables:
      - `HIVE_RUNTIME_VERSION`: `hive3`
      - `RANGER_SERVER_URL`: Ranger admin URL
      - `RANGER_HIVE_REPOSITORY_NAME`: Hive repository name in Ranger admin
      - `RANGER_HDFS_REPOSITORY_NAME`: HDFS repository name in Ranger admin
    - If you want to enable Hive Ranger plugin, you need both set the `RANGER_SERVER_URL` and `RANGER_HIVE_REPOSITORY_NAME` environment variables. Hive Ranger audit logs are stored in the `/tmp/root/ranger-hive-audit.log`.
    - If you want to enable HDFS Ranger plugin, you need both set the `RANGER_SERVER_URL` and `RANGER_HDFS_REPOSITORY_NAME` environment variables. HDFS Ranger audit logs are stored in the `/usr/local/hadoop/logs/ranger-hdfs-audit.log`
    - Example: docker run -e HIVE_RUNTIME_VERSION='hive3' -e RANGER_SERVER_URL='http://ranger-server:6080' -e RANGER_HIVE_REPOSITORY_NAME='hiveDev' -e RANGER_HDFS_REPOSITORY_NAME='hdfsDev' ... datastrato/gravitino-ci-hive:0.1.13

- datastrato/gravitino-ci-hive:0.1.12
  - Shrink hive Docker image size by 420MB

- datastrato/gravitino-ci-hive:0.1.11
  - Remove `yarn` from the startup script; Remove `yarn-site.xml` and `yarn-env.sh` files;
  - Change the value of `mapreduce.framework.name` from `yarn` to `local` in the `mapred-site.xml` file. 

- datastrato/gravitino-ci-hive:0.1.10
  - Remove SSH service from the startup script.
  - Use `hadoop-daemon.sh` to start HDFS services.

- datastrato/gravitino-ci-hive:0.1.9
  - Remove cache after installing packages.

- datastrato/gravitino-ci-hive:0.1.8
  - Change the value of `hive.server2.enable.doAs` to `true`

- datastrato/gravitino-ci-hive:0.1.7
  - Download MySQL JDBC driver before building the Docker image
  - Set `hdfs` as HDFS superuser group

- datastrato/gravitino-ci-hive:0.1.6
  - No starting YARN when container startup
  - Removed expose ports:
    - `22` SSH
    - `8088` YARN Service

- datastrato/gravitino-ci-hive:0.1.5
  - Rollback `Map container hostname to 127.0.0.1 before starting Hadoop` of `datastrato/gravitino-ci-hive:0.1.4`

- datastrato/gravitino-ci-hive:0.1.4
  - Configure HDFS DataNode data transfer address to be `0.0.0.0:50010`
  - Map the container hostname to `127.0.0.1` before starting Hadoop
  - Expose `50010` port for the HDFS DataNode

- datastrato/gravitino-ci-hive:0.1.3
  - Change MySQL bind-address from `127.0.0.1` to `0.0.0.0`
  - Add `iceberg` to MySQL users with password `iceberg`
  - Export `3306` port for MySQL

- datastrato/gravitino-ci-hive:0.1.2
  - Based on `datastrato/gravitino-ci-hive:0.1.1`
  - Modify `fs.defaultFS` from `local` to `0.0.0.0` in the `core-site.xml` file.
  - Expose `9000` port in the `Dockerfile` file.

- datastrato/gravitino-ci-hive:0.1.1
  - Based on `datastrato/gravitino-ci-hive:0.1.0`
  - Modify HDFS/YARN/HIVE `MaxPermSize` from `8GB` to `128MB`
  - Modify `HADOOP_HEAPSIZE` from `8192` to `128`

- datastrato/gravitino-ci-hive:0.1.0
  - Docker image `datastrato/gravitino-ci-hive:0.1.0`
  - `hadoop-2.7.3`
  - `hive-2.3.9`
  - Expose ports:
    - `22` SSH
    - `9000` HDFS defaultFS
    - `50070` HDFS NameNode
    - `50075` HDFS DataNode HTTP server
    - `50010` HDFS DataNode data transfer
    - `8088` YARN Service
    - `9083` Hive metastore
    - `10000` HiveServer2
    - `10002` HiveServer2 HTTP

## Apache Gravitino CI Trino image

You can use this image to test Trino.

Changelog

- apache/gravitino-ci:trino-0.1.6 (Switch to Apache official DockerHub repository)
  - Use Gravitino release 0.6.0 Dockerfile to build the image.

- datastrato/gravitino-ci-trino:0.1.6
  - Upgrade trino:426 to trino:435

- datastrato/gravitino-ci-trino:0.1.5
  - Add check for the version of gravitino-trino-connector

- datastrato/gravitino-ci-trino:0.1.4
  - Change `-Xmx1G` to `-Xmx2G` in the config file `/etc/trino/jvm.config`

- datastrato/gravitino-ci-trino:0.1.3
  - Remove copy content in folder `gravitino-trino-connector` to plugin folder `/usr/lib/trino/plugin/gravitino`

- datastrato/gravitino-ci-trino:0.1.2
  - Copy JDBC driver 'mysql-connector-java' and 'postgres' to `/usr/lib/trino/iceberg/` folder

- datastrato/gravitino-ci-trino:0.1.0
  - Docker image `datastrato/gravitino-ci-trino:0.1.0`
  - Based on `trinodb/trino:426` and removed some unused plugins from it.
  - Expose ports:
    - `8080` Trino JDBC port

## Apache Gravitino CI Doris image

You can use this image to test Apache Doris.

Changelog

- apache/gravitino-ci:doris-0.1.5 (Switch to Apache official DockerHub repository)
  - Use Gravitino release 0.6.0 Dockerfile to build the image.

- datastrato/gravitino-ci-doris:0.1.5
  - Remove the chmod command in the Dockerfile to decrease the size of the Docker image.

- datastrato/gravitino-ci-doris:0.1.4
  - remove chmod in start.sh to accelerate the startup speed

- datastrato/gravitino-ci-doris:0.1.3
  - To adapt to the CI framework, don't exit container when start failed, logs are no longer printed to stdout. 
  - Add `report_disk_state_interval_seconds` config to decrease report interval.

- datastrato/gravitino-ci-doris:0.1.2
  - Add a check for the status of Doris BE, add retry for adding BE nodes.

- datastrato/gravitino-ci-doris:0.1.1
  - Optimize `start.sh`, add disk space check before starting Doris, exit when FE or BE start failed, add log to stdout

- datastrato/gravitino-ci-doris:0.1.0
  - Docker image `datastrato/gravitino-ci-doris:0.1.0`
  - Start Doris BE & FE in one container
  - Please set table properties `"replication_num" = "1"` when creating a table in Doris, because the default replication number is 3, but the Doris container only has one BE.
  - Username: `root`, Password: N/A (password is empty)
  - Expose ports:
    - `8030` Doris FE HTTP port
    - `9030` Doris FE MySQL server port

## Apache Gravitino CI Apache Ranger image

You can use this image to control Trino's permissions.

Changelog

- apache/gravitino-ci:ranger-0.1.2
  - Build ranger packages from source.

- apache/gravitino-ci:ranger-0.1.1 (Switch to Apache official DockerHub repository)
  - Use Gravitino release 0.6.0 Dockerfile to build the image.

- datastrato/gravitino-ci-ranger:0.1.1
  - Docker image datastrato/gravitino-ci-ranger:0.1.1
  - Use `ranger-admin` release from `datastrato/apache-ranger:2.4.0` to build docker image.
  - Remove unnecessary hack in `start-ranger-service.sh`.
  - Reduce docker image build time from `~1h` to `~5min`.
  - How to debug Ranger admin service:
    - Use `docker exec -it <container_id> bash` to enter the docker container.
    - Add these context `export JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5001` into `/opt/ranger-admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-env-debug.sh` in the docker container.
    - Execute `./opt/ranger-admin/stop-ranger-admin.sh` and `./opt/ranger-admin/start-ranger-admin.sh` to restart Ranger admin.
    - Clone the `Apache Ranger` project from GiHub and checkout the `2.4.0` release.
    - Create a remote debug configuration (`Use model classpath` = `EmbeddedServer`) in your IDE and connect to the Ranger admin container.

- datastrato/gravitino-ci-ranger:0.1.0
  - Docker image `datastrato/gravitino-ci-ranger:0.1.0`
  - Support Apache Ranger 2.4.0
  - Use environment variable `RANGER_PASSWORD` to set up Apache Ranger admin password, please 
    notice Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
  - Expose ports:
    - `6080` Apache Ranger admin port
