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

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The endpoints file, as typed records: the single declaration of every key an external service
 * reads. {@link ExternalEndpoints} binds the YAML file and the flat overrides to it and rejects any
 * other key; the JSON Schema, the example file and the docs table are generated from or checked
 * against it.
 *
 * <p>A new service is a component here plus a record implementing {@link Service}.
 */
@JsonClassDescription(
    "Services that Gravitino integration tests reach instead of starting Testcontainers")
public record TestEndpoints(
    @JsonProperty("containers")
        @JsonPropertyDescription(
            "external makes every service external, so the run needs no Docker; default managed")
        @Nullable
        Mode containers,
    @JsonProperty("hive") @Nullable Hive hive,
    @JsonProperty("kafka") @Nullable Kafka kafka,
    @JsonProperty("mysql") @Nullable MySQL mysql) {

  /** Whether services run as Testcontainers or elsewhere. */
  public enum Mode {
    @JsonProperty("managed")
    MANAGED,
    @JsonProperty("external")
    EXTERNAL
  }

  /** Marks a key that an external service cannot run without. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
  public @interface Required {}

  /** The keys every service has besides its own. */
  public interface Service {
    /**
     * Returns whether the service is external.
     *
     * @return true to reach it instead of starting a container; null means false
     */
    @Nullable
    Boolean external();

    /**
     * Returns the named instances of the service, each a whole block of the service's keys.
     *
     * @return the instances, by name
     */
    Map<String, ? extends Service> instances();
  }

  /** Database services: what tests connect as. */
  public interface Database extends Service {
    /**
     * Returns the user tests connect as.
     *
     * @return the user
     */
    String user();

    /**
     * Returns the password of {@link #user()}.
     *
     * @return the password
     */
    String password();
  }

  /** A Hive metastore and the HDFS its warehouse lives on. */
  @JsonClassDescription("A Hive metastore and the HDFS its warehouse lives on")
  public record Hive(
      @JsonProperty("external") @JsonPropertyDescription(EXTERNAL) @Nullable Boolean external,
      @JsonProperty("metastore-uri")
          @JsonPropertyDescription("thrift:// address of the Hive metastore")
          @Required
          String metastoreUri,
      @JsonProperty("hdfs-uri")
          @JsonPropertyDescription("hdfs:// address of the HDFS NameNode the metastore uses")
          @Required
          String hdfsUri,
      @JsonProperty("hdfs-client-conf")
          @JsonPropertyDescription(
              "Hadoop client settings for the test JVM, such as"
                  + " dfs.client.use.datanode.hostname: \"true\"")
          Map<String, String> hdfsClientConf,
      @JsonProperty("hadoop-user")
          @JsonPropertyDescription("HDFS user that tests act as; default root")
          @Nullable
          String hadoopUser,
      @JsonProperty("instances") @JsonPropertyDescription(INSTANCES) Map<String, Hive> instances)
      implements Service {
    public Hive {
      hdfsClientConf = hdfsClientConf == null ? Map.of() : Map.copyOf(hdfsClientConf);
      instances = instances == null ? Map.of() : Map.copyOf(instances);
    }
  }

  /** Kafka brokers. */
  @JsonClassDescription("Kafka brokers")
  public record Kafka(
      @JsonProperty("external") @JsonPropertyDescription(EXTERNAL) @Nullable Boolean external,
      @JsonProperty("bootstrap-servers")
          @JsonPropertyDescription("host:port of each broker")
          @Required
          List<String> bootstrapServers,
      @JsonProperty("instances") @JsonPropertyDescription(INSTANCES) Map<String, Kafka> instances)
      implements Service {
    public Kafka {
      bootstrapServers = bootstrapServers == null ? null : List.copyOf(bootstrapServers);
      instances = instances == null ? Map.of() : Map.copyOf(instances);
    }
  }

  /** A MySQL server. */
  @JsonClassDescription("A MySQL server")
  public record MySQL(
      @JsonProperty("external") @JsonPropertyDescription(EXTERNAL) @Nullable Boolean external,
      @JsonProperty("jdbc-url")
          @JsonPropertyDescription("jdbc:mysql://host:port of the server, without a database")
          @Required
          String jdbcUrl,
      @JsonProperty("user")
          @JsonPropertyDescription("user that tests connect and create databases as")
          @Required
          String user,
      @JsonProperty("password") @JsonPropertyDescription("password of that user") @Required
          String password,
      @JsonProperty("instances") @JsonPropertyDescription(INSTANCES) Map<String, MySQL> instances)
      implements Database {
    public MySQL {
      instances = instances == null ? Map.of() : Map.copyOf(instances);
    }

    // Logged when the container is skipped; keep the password out of the log.
    @Override
    public String toString() {
      return "MySQL[jdbc-url=" + jdbcUrl + ", user=" + user + "]";
    }
  }

  private static final String EXTERNAL =
      "true to reach the service instead of starting a container";
  private static final String INSTANCES =
      "Named instances, for tests that need more than one; an instance not listed uses the"
          + " service's own keys";
}
