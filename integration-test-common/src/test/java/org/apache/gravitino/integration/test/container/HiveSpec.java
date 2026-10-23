/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;

/**
 * A Hive server of one version, authentication mode and storage.
 *
 * <p>Plain and Kerberos Hive run side by side, but each holds one version at a time: asking for a
 * different version stops the running Hive of that mode and starts the requested one (see {@code
 * ContainerSlot}). Tests of one version therefore share a container, and a version switch costs a
 * restart. A private Hive ({@link Scope#CLASS} or {@link Scope#METHOD}) leaves the shared one
 * alone. A Hive with cloud {@link HiveStorage} is a third container, one storage and version at a
 * time.
 */
public record HiveSpec(HiveVersion version, HiveAuth auth, HiveStorage storage)
    implements ContainerSpec<HiveContainer> {

  private static final String GCS_ACCOUNT_FILE_IN_CONTAINER = "/tmp/gcs-service-account.json";

  /**
   * Checks the combination.
   *
   * @throws IllegalArgumentException if Ranger is asked for with a Hive other than Hive3, or cloud
   *     storage with authentication
   */
  public HiveSpec {
    if (auth == HiveAuth.RANGER && version != HiveVersion.HIVE3) {
      throw new IllegalArgumentException("Ranger Hive runs Hive3 only, not " + version);
    }
    if (storage != HiveStorage.HDFS && auth != HiveAuth.NONE) {
      throw new IllegalArgumentException(
          "Hive with " + storage + " storage supports no authentication, not " + auth);
    }
  }

  /**
   * A Hive that keeps its tables in its own HDFS.
   *
   * @param version the Hive version
   * @param auth the authentication mode
   */
  public HiveSpec(HiveVersion version, HiveAuth auth) {
    this(version, auth, HiveStorage.HDFS);
  }

  @Override
  public List<ContainerSpec<?>> dependencies() {
    if (auth == HiveAuth.RANGER) {
      return List.of(new RangerSpec());
    }
    return storage == HiveStorage.S3 ? List.of(new LocalStackSpec()) : List.of();
  }

  @Override
  public Class<HiveContainer> containerType() {
    return HiveContainer.class;
  }

  @Override
  public HiveContainer getOrStart(SharedContainers shared) {
    if (storage != HiveStorage.HDFS) {
      return shared.startStorageHiveContainer(storage, storageEnv(shared), storageFiles());
    }
    return switch (auth) {
      case NONE -> {
        shared.startHiveContainer(env());
        yield shared.getHiveContainer();
      }
      case KERBEROS -> {
        shared.startKerberosHiveContainer(env());
        yield shared.getKerberosHiveContainer();
      }
      case RANGER -> {
        shared.startHiveRangerContainer(rangerEnv(new RangerSpec().getOrStart(shared)));
        yield shared.getHiveRangerContainer();
      }
    };
  }

  private Map<String, String> storageEnv(SharedContainers shared) {
    ImmutableMap.Builder<String, String> env = ImmutableMap.<String, String>builder().putAll(env());
    switch (storage) {
      case S3 -> {
        GravitinoLocalStackContainer localStack = new LocalStackSpec().getOrStart(shared);
        env.put("S3_ACCESS_KEY", GravitinoLocalStackContainer.ACCESS_KEY)
            .put("S3_SECRET_KEY", GravitinoLocalStackContainer.SECRET_KEY)
            .put(
                "S3_ENDPOINT",
                String.format(
                    "http://%s:%d",
                    localStack.getContainerIpAddress(), GravitinoLocalStackContainer.PORT));
      }
      case GCS -> env.put("SERVICE_ACCOUNT_FILE", GCS_ACCOUNT_FILE_IN_CONTAINER);
      case ABS -> env.put("ABS_ACCOUNT_NAME", requiredEnv("ABS_ACCOUNT_NAME"))
          .put("ABS_ACCOUNT_KEY", requiredEnv("ABS_ACCOUNT_KEY"));
      case HDFS -> {}
    }
    return env.build();
  }

  private Map<String, String> storageFiles() {
    return storage == HiveStorage.GCS
        ? Map.of(GCS_ACCOUNT_FILE_IN_CONTAINER, requiredEnv("GCS_SERVICE_ACCOUNT_JSON_PATH"))
        : Map.of();
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(name + " must be set for this Hive storage");
    }
    return value;
  }

  private Map<String, String> rangerEnv(RangerContainer ranger) {
    return ImmutableMap.<String, String>builder()
        .putAll(env())
        .put(
            RangerContainer.DOCKER_ENV_RANGER_SERVER_URL,
            String.format(
                "http://%s:%d", ranger.getContainerIpAddress(), RangerContainer.RANGER_SERVER_PORT))
        .put(
            RangerContainer.DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME, RangerContainer.HIVE_REPOSITORY)
        .put(
            RangerContainer.DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME, RangerContainer.HDFS_REPOSITORY)
        .put(HiveContainer.HADOOP_USER_NAME, RangerContainer.HADOOP_USER)
        .build();
  }

  // Keeps the shared Hive's host name: the Kerberos image derives its principals from it. On macOS
  // the shared network then has two containers with one alias, which is fine because tests reach
  // containers by IP; on Linux CI there is no shared network.
  @Override
  public boolean supportsPrivate() {
    return storage == HiveStorage.HDFS && auth != HiveAuth.RANGER;
  }

  @Override
  public HiveContainer startPrivate(SharedContainers shared, Durability durability) {
    if (storage != HiveStorage.HDFS) {
      throw new UnsupportedOperationException(
          "scope CLASS/METHOD is not supported for Hive with " + storage + " storage yet");
    }
    return shared.startPrivate(
        network -> {
          HiveContainer container = builder().withNetwork(network).build();
          container.setDurability(durability);
          return container;
        });
  }

  private HiveContainer.Builder builder() {
    return switch (auth) {
      case NONE -> SharedContainers.hiveBuilder(env());
      case KERBEROS -> SharedContainers.kerberosHiveBuilder(env());
      case RANGER -> throw new UnsupportedOperationException(
          "scope CLASS/METHOD is not supported for Ranger Hive yet");
    };
  }

  private Map<String, String> env() {
    return Map.of(HiveContainer.HIVE_RUNTIME_VERSION, version.runtimeVersion());
  }

  static final class Factory implements ContainerFixture.SpecFactory<HiveFixture> {
    @Override
    public Durability durabilityFor(HiveFixture fixture) {
      return fixture.durability();
    }

    @Override
    public Startup startupFor(HiveFixture fixture) {
      // Ranger's services must exist before a Ranger-enabled Hive reports to them, and tests
      // create them in @BeforeAll, so that Hive starts when a test first asks for it.
      return fixture.auth() == HiveAuth.RANGER ? Startup.LAZY : fixture.startup();
    }

    @Override
    public HiveSpec specFor(HiveFixture fixture) {
      return new HiveSpec(fixture.version(), fixture.auth(), fixture.storage());
    }

    @Override
    public Scope scopeFor(HiveFixture fixture) {
      return fixture.scope();
    }
  }
}
