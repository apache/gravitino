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
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * A Hive server of one version, authentication mode and storage.
 *
 * <p>Plain and Kerberos Hive run side by side, but each holds one version at a time: asking for a
 * different version stops the running Hive of that mode and starts the requested one (see {@code
 * ContainerSlot}). Tests of one version therefore share a container, and a version switch costs a
 * restart. A Hive with cloud {@link HiveStorage} is a third container, one storage and version at a
 * time. A private Hive ({@link Scope#CLASS} or {@link Scope#METHOD}) of any mode and storage leaves
 * the shared one alone.
 *
 * <p>A Kerberos Hive can create extra principals when it starts ({@link
 * HiveFixture#kerberosPrincipals()}), for tests that log in as users of their own. Such a Hive is
 * always private, and specs with different principals are different containers.
 */
public record HiveSpec(
    HiveVersion version, HiveAuth auth, HiveStorage storage, List<String> kerberosPrincipals)
    implements ContainerSpec<HiveContainer> {

  private static final String GCS_ACCOUNT_FILE_IN_CONTAINER = "/tmp/gcs-service-account.json";
  // A principal's name is also its keytab's file name and goes into the start script unquoted.
  private static final String PRIVATE_HINT = "use scope = Scope.CLASS or Scope.METHOD";
  private static final Pattern PRINCIPAL_NAME = Pattern.compile("[A-Za-z0-9_.-]+");

  /**
   * Checks the combination and sorts the principals, so that the order they are declared in does
   * not make a different container.
   *
   * @throws IllegalArgumentException if Ranger is asked for with a Hive other than Hive3, cloud
   *     storage with authentication, principals without Kerberos, or a principal name with
   *     characters other than letters, digits, {@code _}, {@code .} and {@code -}
   */
  public HiveSpec {
    kerberosPrincipals = List.copyOf(new TreeSet<>(kerberosPrincipals));
    if (!kerberosPrincipals.isEmpty() && auth != HiveAuth.KERBEROS) {
      throw new IllegalArgumentException(
          "Kerberos principals need a Kerberos Hive, not " + auth + ": " + kerberosPrincipals);
    }
    for (String principal : kerberosPrincipals) {
      if (!PRINCIPAL_NAME.matcher(principal).matches()) {
        throw new IllegalArgumentException(
            "A Kerberos principal is a name without realm or instance, not '" + principal + "'");
      }
    }
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

  /**
   * A Hive without extra Kerberos principals.
   *
   * @param version the Hive version
   * @param auth the authentication mode
   * @param storage where Hive keeps table data besides its own HDFS
   */
  public HiveSpec(HiveVersion version, HiveAuth auth, HiveStorage storage) {
    this(version, auth, storage, List.of());
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
    if (!kerberosPrincipals.isEmpty()) {
      // The shared Kerberos Hive has none of them, and adding them would change it for others.
      throw new IllegalStateException(
          "A Hive with Kerberos principals " + kerberosPrincipals + " is private: " + PRIVATE_HINT);
    }
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
    return true;
  }

  // Its dependencies stay shared: a private Ranger Hive reports to the shared Ranger, and a
  // private S3 Hive keeps its tables in the shared LocalStack.
  @Override
  public HiveContainer startPrivate(SharedContainers shared, Durability durability) {
    return shared.startPrivate(builder(shared), durability);
  }

  private HiveContainer.Builder builder(SharedContainers shared) {
    if (storage != HiveStorage.HDFS) {
      return SharedContainers.hiveBuilder(storageEnv(shared)).withFilesToMount(storageFiles());
    }
    return switch (auth) {
      case NONE -> SharedContainers.hiveBuilder(env());
      case KERBEROS -> SharedContainers.kerberosHiveBuilder(env())
          .withKerberosPrincipals(kerberosPrincipals);
      case RANGER -> SharedContainers.hiveRangerBuilder(
          rangerEnv(new RangerSpec().getOrStart(shared)));
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
      HiveSpec spec =
          new HiveSpec(
              fixture.version(),
              fixture.auth(),
              fixture.storage(),
              List.of(fixture.kerberosPrincipals()));
      if (!spec.kerberosPrincipals().isEmpty() && fixture.scope() == Scope.RUN) {
        throw new IllegalArgumentException(
            "@HiveFixture(kerberosPrincipals = ...) needs a container of its own: " + PRIVATE_HINT);
      }
      return spec;
    }

    @Override
    public Scope scopeFor(HiveFixture fixture) {
      return fixture.scope();
    }
  }
}
