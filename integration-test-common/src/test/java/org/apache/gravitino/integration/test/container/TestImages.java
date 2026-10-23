/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Every Docker image the integration tests pin, in one place, so a version bump is a one-line
 * change and a reviewer can see the whole set at a glance.
 *
 * <p>Images that CI swaps out keep their {@code GRAVITINO_CI_*_DOCKER_IMAGE} environment variable
 * as an override. The fallbacks here match the values {@code build.gradle.kts} passes to the test
 * JVM, so a Gradle run picks the same image as before; when Gradle (or the shell) sets the
 * variable, that value wins. The fallbacks let a test started from an IDE without those variables
 * still find its image.
 */
public final class TestImages {

  /** Hive with HDFS, the default image for {@link HiveContainer}. */
  public static final String HIVE =
      env("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "apache/gravitino-ci:hive-0.1.20");

  /** Hive with HDFS and a KDC, for Kerberos tests. */
  public static final String KERBEROS_HIVE =
      env("GRAVITINO_CI_KERBEROS_HIVE_DOCKER_IMAGE", "apache/gravitino-ci:kerberos-hive-0.1.7");

  /** Doris 1.2 all-in-one image, the default for {@link DorisContainer}. */
  public static final String DORIS =
      env("GRAVITINO_CI_DORIS_DOCKER_IMAGE", "apache/gravitino-ci:doris-0.1.5");

  /** Official Doris 2.1 all-in-one image. */
  public static final String DORIS_2_1 = "apache/doris:doris-all-in-one-2.1.0";

  /** Official Doris 3.0 FE image; {@link DorisContainer} derives the matching BE image. */
  public static final String DORIS_3_0 = "apache/doris:fe-3.0.6.2";

  /** Official Doris 4.0 FE image; {@link DorisContainer} derives the matching BE image. */
  public static final String DORIS_4_0 = "apache/doris:fe-4.0.6";

  /** Trino with the Gravitino connector layout. */
  public static final String TRINO =
      env("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "apache/gravitino-ci:trino-0.1.6");

  /** Ranger admin. */
  public static final String RANGER =
      env("GRAVITINO_CI_RANGER_DOCKER_IMAGE", "apache/gravitino-ci:ranger-0.1.2");

  /** Kafka in KRaft mode. */
  public static final String KAFKA = env("GRAVITINO_CI_KAFKA_DOCKER_IMAGE", "apache/kafka:3.7.0");

  /** LocalStack for AWS service emulation. */
  public static final String LOCALSTACK =
      env("GRAVITINO_CI_LOCALSTACK_DOCKER_IMAGE", "localstack/localstack:4.14.0");

  /** Moto for AWS service emulation; Gradle does not set this variable. */
  public static final String MOTO = env("GRAVITINO_CI_MOTO_DOCKER_IMAGE", "motoserver/moto:5.1.4");

  /** MySQL 8.0, the default for {@link MySQLContainer}. */
  public static final String MYSQL = "mysql:8.0";

  /** MySQL 5.7, for tests that need the older server. */
  public static final String MYSQL_5_7 = "mysql:5.7";

  /** PostgreSQL 12. */
  public static final String POSTGRESQL_12 = "postgres:12";

  /** PostgreSQL 13, the default for {@link PostgreSQLContainer}. */
  public static final String POSTGRESQL_13 = "postgres:13";

  /** PostgreSQL 14. */
  public static final String POSTGRESQL_14 = "postgres:14";

  /** PostgreSQL 15. */
  public static final String POSTGRESQL_15 = "postgres:15";

  /** PostgreSQL 16. */
  public static final String POSTGRESQL_16 = "postgres:16";

  /** ClickHouse server; Gradle does not set this variable. */
  public static final String CLICKHOUSE =
      env("GRAVITINO_CLICKHOUSE_TEST_IMAGE", "clickhouse:24.8.14");

  /** OceanBase community edition. */
  public static final String OCEANBASE = "oceanbase/oceanbase-ce:4.2.1-lts";

  /** StarRocks all-in-one (FE and BE in one container). */
  public static final String STARROCKS = "starrocks/allin1-ubuntu:3.3.22";

  /** ZooKeeper. */
  public static final String ZOOKEEPER = "zookeeper:3.8.0";

  /**
   * RustFS, pinned by digest in {@code docker-compose-rustfs.yml} so the image-update tooling can
   * bump it. Parsed here, not in {@link RustFSContainer}, so reading any image never loads the AWS
   * SDK classes RustFSContainer needs.
   */
  public static final String RUSTFS = loadRustFSImage();

  private TestImages() {}

  static String rustFSImageFromCompose(String compose) {
    // This single-service manifest is deliberately limited to one unquoted image reference.
    // Fail closed if its structure changes, rather than silently using a different image pin.
    Matcher matcher = Pattern.compile("(?m)^\\s+image: (\\S+)\\s*$").matcher(compose);
    if (!matcher.find()) {
      throw new IllegalArgumentException("RustFS Compose manifest must contain an image");
    }
    String image = matcher.group(1);
    if (matcher.find() || !image.matches("rustfs/rustfs:[^@\\s]+@sha256:[0-9a-f]{64}")) {
      throw new IllegalArgumentException(
          "RustFS Compose manifest must contain one version and digest pin");
    }
    return image;
  }

  private static String loadRustFSImage() {
    try (InputStream stream = TestImages.class.getResourceAsStream("/docker-compose-rustfs.yml")) {
      if (stream == null) {
        throw new IllegalStateException("Missing docker-compose-rustfs.yml test resource");
      }
      return rustFSImageFromCompose(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read RustFS image pin", e);
    }
  }

  private static String env(String variable, String fallback) {
    return resolve(System::getenv, variable, fallback);
  }

  // Takes the lookup as a function so the precedence rule can be tested without the environment.
  static String resolve(Function<String, String> lookup, String variable, String fallback) {
    String value = lookup.apply(variable);
    return value == null || value.isBlank() ? fallback : value;
  }
}
