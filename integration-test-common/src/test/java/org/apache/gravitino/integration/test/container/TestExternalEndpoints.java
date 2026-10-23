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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jackson.JacksonOption;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests how external endpoints are read and checked, and that the schema, the example file and the
 * docs table match {@link TestEndpoints}. Run with {@code -Pgravitino.test.updateGenerated=true} to
 * rewrite the schema and the docs table.
 */
public class TestExternalEndpoints {

  private static final Path RESOURCES = Path.of("integration-test-common/src/test/resources");
  private static final Path SCHEMA = RESOURCES.resolve("test-endpoints.schema.json");
  private static final Path EXAMPLE = RESOURCES.resolve("external-endpoints.example.yaml");
  private static final Path PROFILES = Path.of("dev/test-endpoints");
  private static final Path DOC =
      Path.of("design-docs", "testing", "containers", "external-endpoints.md");
  private static final String BEGIN = "<!-- BEGIN GENERATED: external keys -->";
  private static final String END = "<!-- END GENERATED: external keys -->";
  private static final boolean UPDATE = Boolean.getBoolean("gravitino.test.updateGenerated");

  private static final String HIVE_FILE =
      "containers: external\n"
          + "hive:\n"
          + "  metastore-uri: thrift://from-file:9083\n"
          + "  hdfs-uri: hdfs://from-file:9000\n";

  @TempDir Path dir;

  private TestEndpoints read(String yaml, Map<String, String> properties, Map<String, String> env)
      throws IOException {
    Map<String, String> all = new HashMap<>(properties);
    if (yaml != null) {
      Path file = dir.resolve("endpoints.yaml");
      Files.writeString(file, yaml);
      all.put(ExternalEndpoints.FILE_KEY, file.toString());
    }
    return ExternalEndpoints.read(all, env);
  }

  private static <T extends TestEndpoints.Service> T external(
      TestEndpoints endpoints, String service, String instance) {
    return ExternalEndpoints.<T>forService(endpoints, service, instance).orElseThrow();
  }

  @Test
  void testManagedUnlessDeclaredExternal() throws IOException {
    Assertions.assertTrue(
        ExternalEndpoints.forService(read(null, Map.of(), Map.of()), "kafka", "").isEmpty());
    Map<String, String> brokers = Map.of("gravitino.test.kafka.bootstrap-servers", "b:9092");
    Assertions.assertTrue(
        ExternalEndpoints.forService(
                read(null, brokers, Map.of("GRAVITINO_TEST_KAFKA_EXTERNAL", "true")), "kafka", "")
            .isPresent());
    Assertions.assertTrue(
        ExternalEndpoints.forService(read("containers: external\n", brokers, Map.of()), "kafka", "")
            .isPresent());
    Assertions.assertTrue(
        ExternalEndpoints.forService(
                read(null, brokers, Map.of("GRAVITINO_TEST_HIVE_EXTERNAL", "true")), "kafka", "")
            .isEmpty());
    // Other gravitino.test.* settings are not endpoint keys.
    Assertions.assertDoesNotThrow(
        () -> read(null, Map.of("gravitino.test.durableContainers", "true"), Map.of()));
  }

  @Test
  void testPropertyBeatsEnvironmentBeatsFile() throws IOException {
    Map<String, String> env = Map.of("GRAVITINO_TEST_HIVE_METASTORE_URI", "thrift://from-env:9083");
    Map<String, String> properties =
        Map.of("gravitino.test.hive.metastore-uri", "thrift://from-property:9083");

    TestEndpoints.Hive hive = external(read(HIVE_FILE, properties, env), "hive", "");
    Assertions.assertEquals("thrift://from-property:9083", hive.metastoreUri());
    Assertions.assertEquals("hdfs://from-file:9000", hive.hdfsUri());
    Assertions.assertNull(hive.hadoopUser());

    hive = external(read(HIVE_FILE, Map.of(), env), "hive", "");
    Assertions.assertEquals("thrift://from-env:9083", hive.metastoreUri());
  }

  @Test
  void testFlatListsAndMaps() throws IOException {
    TestEndpoints.Kafka kafka =
        external(
            read(
                "kafka: {external: true, bootstrap-servers: [a:9092]}\n",
                Map.of("gravitino.test.kafka.bootstrap-servers", "b:9092, c:9092"),
                Map.of()),
            "kafka",
            "");
    Assertions.assertEquals(List.of("b:9092", "c:9092"), kafka.bootstrapServers());

    TestEndpoints.Hive hive =
        external(
            read(
                HIVE_FILE,
                Map.of(),
                Map.of(
                    "GRAVITINO_TEST_HIVE_HDFS_CLIENT_CONF",
                    "dfs.client.use.datanode.hostname = true, dfs.replication=1")),
            "hive",
            "");
    Assertions.assertEquals(
        Map.of("dfs.client.use.datanode.hostname", "true", "dfs.replication", "1"),
        hive.hdfsClientConf());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> read(null, Map.of("gravitino.test.hive.hdfs-client-conf", "novalue"), Map.of()));
  }

  @Test
  void testMissingRequiredKeysFailNamingEveryWayToSetThem() throws IOException {
    TestEndpoints endpoints = read("hive: {external: true}\n", Map.of(), Map.of());
    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class, () -> ExternalEndpoints.forService(endpoints, "hive", ""));
    String message = e.getMessage();
    Assertions.assertTrue(message.contains("2 more key(s)"), message);
    Assertions.assertTrue(message.contains("hive.metastore-uri"), message);
    Assertions.assertTrue(message.contains("-Pgravitino.test.hive.metastore-uri="), message);
    Assertions.assertTrue(message.contains("GRAVITINO_TEST_HIVE_METASTORE_URI"), message);
    Assertions.assertTrue(message.contains("hive.hdfs-uri"), message);
    Assertions.assertFalse(message.contains("hadoop-user"), message);
  }

  @Test
  void testTypoKeyFailsFastWithASuggestion() {
    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> read(HIVE_FILE + "  metastore-url: thrift://x\n", Map.of(), Map.of()));
    String message = e.getMessage();
    Assertions.assertTrue(message.contains("key hive.metastore-url (the file "), message);
    Assertions.assertTrue(message.contains("did you mean metastore-uri?"), message);
    Assertions.assertTrue(
        message.contains(
            "Valid keys there: external, hadoop-user, hdfs-client-conf, hdfs-uri, instances,"
                + " metastore-uri"),
        message);

    // A typo fails even while the service is managed, since it may be the key that meant
    // "external".
    e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> read(null, Map.of("gravitino.test.kafka.externl", "true"), Map.of()));
    Assertions.assertTrue(e.getMessage().contains("did you mean external?"), e.getMessage());

    e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> read(null, Map.of(), Map.of("GRAVITINO_TEST_KAFKA_BOOTSTRAP_SERVER", "b:9092")));
    Assertions.assertTrue(
        e.getMessage().contains("did you mean bootstrap-servers?"), e.getMessage());

    e =
        Assertions.assertThrows(
            IllegalStateException.class, () -> read("kafak: {}\n", Map.of(), Map.of()));
    Assertions.assertTrue(e.getMessage().contains("did you mean kafka?"), e.getMessage());
  }

  @Test
  void testInstanceReadsItsBlockElseFallsBackToTheService() throws IOException {
    String file =
        HIVE_FILE
            + "  instances:\n"
            + "    east:\n"
            + "      metastore-uri: thrift://east:9083\n"
            + "      hdfs-uri: hdfs://east:9000\n";
    TestEndpoints endpoints = read(file, Map.of(), Map.of());

    TestEndpoints.Hive east = external(endpoints, "hive", "east");
    Assertions.assertEquals("thrift://east:9083", east.metastoreUri());
    Assertions.assertEquals("hdfs://east:9000", east.hdfsUri());
    // No "west" instance, so west reads the service's own keys.
    TestEndpoints.Hive west = external(endpoints, "hive", "west");
    Assertions.assertEquals("thrift://from-file:9083", west.metastoreUri());
    Assertions.assertEquals(
        "thrift://from-file:9083",
        ExternalEndpoints.<TestEndpoints.Hive>forService(endpoints, "hive", "")
            .orElseThrow()
            .metastoreUri());

    // Flat keys reach an instance too.
    TestEndpoints.Hive overridden =
        external(
            read(
                file,
                Map.of("gravitino.test.hive.east.metastore-uri", "thrift://p:9083"),
                Map.of("GRAVITINO_TEST_HIVE_EAST_HDFS_URI", "hdfs://e:9000")),
            "hive",
            "east");
    Assertions.assertEquals("thrift://p:9083", overridden.metastoreUri());
    Assertions.assertEquals("hdfs://e:9000", overridden.hdfsUri());

    // An instance block is used whole: it does not borrow a missing key from the service.
    TestEndpoints partial =
        read(
            HIVE_FILE + "  instances: {east: {metastore-uri: thrift://east:9083}}\n",
            Map.of(),
            Map.of());
    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> ExternalEndpoints.forService(partial, "hive", "east"));
    Assertions.assertTrue(e.getMessage().contains("hive.instances.east.hdfs-uri"), e.getMessage());
  }

  @Test
  void testExampleAndProfilesBindAndSetEveryDeclaredKey() throws Exception {
    Path example = repoRoot().resolve(EXAMPLE);
    Assertions.assertTrue(
        Files.readString(example)
            .contains("# yaml-language-server: $schema=test-endpoints.schema.json"),
        EXAMPLE + " lacks the schema modeline");
    TestEndpoints endpoints =
        ExternalEndpoints.read(Map.of(ExternalEndpoints.FILE_KEY, example.toString()), Map.of());
    for (Map.Entry<String, RecordComponent> service : services().entrySet()) {
      TestEndpoints.Service block =
          ExternalEndpoints.forService(endpoints, service.getKey(), "").orElseThrow();
      for (Map.Entry<String, RecordComponent> key :
          ExternalEndpoints.keys(service.getValue().getType()).entrySet()) {
        if (!key.getKey().equals("instances")) {
          Object value = key.getValue().getAccessor().invoke(block);
          Assertions.assertNotNull(
              value, EXAMPLE + " lacks " + service.getKey() + "." + key.getKey());
        }
      }
    }
    try (Stream<Path> profiles = Files.list(repoRoot().resolve(PROFILES))) {
      for (Path profile : profiles.filter(p -> p.toString().endsWith(".yaml")).toList()) {
        Assertions.assertDoesNotThrow(
            () ->
                ExternalEndpoints.read(
                    Map.of(ExternalEndpoints.FILE_KEY, profile.toString()), Map.of()),
            profile.toString());
      }
    }
  }

  @Test
  void testSchemaMatchesTheRecords() throws IOException {
    checkGenerated(SCHEMA, schema());
  }

  @Test
  void testDocTableMatchesTheRecords() throws IOException {
    String text = Files.readString(repoRoot().resolve(DOC));
    int begin = text.indexOf(BEGIN);
    int end = text.indexOf(END);
    Assertions.assertTrue(begin >= 0 && end > begin, DOC + " lacks the generated markers");
    checkGenerated(
        DOC,
        text.substring(0, begin + BEGIN.length()) + "\n" + markdownTable() + text.substring(end));
  }

  @Test
  void testExternalContainerHasNoDockerOperations() {
    HiveContainer hive =
        new ExternalHive(
            new TestEndpoints.Hive(
                true,
                "thrift://hms.example:9083",
                "hdfs://nn.example:9000",
                Map.of("dfs.client.use.datanode.hostname", "true"),
                null,
                null));
    Assertions.assertTrue(hive.isExternal());
    Assertions.assertEquals("thrift://hms.example:9083", hive.metastoreUri());
    Assertions.assertEquals("hdfs://nn.example:9000", hive.hdfsUri());
    Assertions.assertEquals("hdfs://nn.example:9000", hive.hdfsConfiguration().get("fs.defaultFS"));
    Assertions.assertEquals(
        "true", hive.hdfsConfiguration().get("dfs.client.use.datanode.hostname"));
    Assertions.assertEquals("root", hive.getHadoopUser());
    Assertions.assertDoesNotThrow(hive::start);
    Assertions.assertDoesNotThrow(hive::stop);
    UnsupportedOperationException e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> hive.executeInContainer("ls"));
    Assertions.assertTrue(e.getMessage().contains("external"), e.getMessage());
    Assertions.assertThrows(UnsupportedOperationException.class, hive::getContainerIpAddress);
    Assertions.assertThrows(UnsupportedOperationException.class, hive::getContainer);
  }

  /**
   * Generates the JSON Schema of the endpoints file from {@link TestEndpoints}. A service's
   * required keys are required once it sets {@code external: true}.
   */
  static String schema() throws IOException {
    SchemaGeneratorConfigBuilder config =
        new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
            .with(new JacksonModule(JacksonOption.FLATTENED_ENUMS_FROM_JSONPROPERTY))
            .with(
                Option.FORBIDDEN_ADDITIONAL_PROPERTIES_BY_DEFAULT,
                Option.MAP_VALUES_AS_ADDITIONAL_PROPERTIES);
    config
        .forTypesInGeneral()
        .withTypeAttributeOverride(
            (node, scope, context) -> {
              Class<?> type = scope.getType().getErasedType();
              if (!type.isRecord() || !TestEndpoints.Service.class.isAssignableFrom(type)) {
                return;
              }
              List<String> required =
                  ExternalEndpoints.keys(type).entrySet().stream()
                      .filter(k -> k.getValue().isAnnotationPresent(TestEndpoints.Required.class))
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toList());
              ObjectNode condition = node.putObject("if");
              condition.putObject("properties").putObject("external").put("const", true);
              condition.putArray("required").add("external");
              required.forEach(node.putObject("then").putArray("required")::add);
            });
    ObjectNode schema = new SchemaGenerator(config.build()).generateSchema(TestEndpoints.class);
    // JSON has no comments, so the licence goes in the schema's own comment keyword.
    ObjectNode ordered = schema.objectNode();
    ordered.set("$schema", schema.remove("$schema"));
    ordered.put("$comment", LICENSE);
    ordered.setAll(schema);
    return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(ordered)
        + "\n";
  }

  /** Generates the docs table: one row per key of each service. */
  static String markdownTable() {
    StringBuilder table =
        new StringBuilder(
            "| YAML key | Property | Environment variable | Required | Description |\n"
                + "| --- | --- | --- | --- | --- |\n");
    for (Map.Entry<String, RecordComponent> top :
        ExternalEndpoints.keys(TestEndpoints.class).entrySet()) {
      Map<String, RecordComponent> keys =
          top.getValue().getType().isRecord()
              ? ExternalEndpoints.keys(top.getValue().getType())
              : Map.of("", top.getValue());
      for (Map.Entry<String, RecordComponent> key : keys.entrySet()) {
        if (key.getKey().equals("instances")) {
          continue;
        }
        String path = key.getKey().isEmpty() ? top.getKey() : top.getKey() + "." + key.getKey();
        String property = ExternalEndpoints.PREFIX + path;
        JsonPropertyDescription description =
            key.getValue().getAccessor().getAnnotation(JsonPropertyDescription.class);
        table.append(
            String.format(
                "| `%s` | `%s` | `%s` | %s | %s |\n",
                path,
                property,
                ExternalEndpoints.envName(property),
                key.getValue().isAnnotationPresent(TestEndpoints.Required.class) ? "yes" : "no",
                description.value().replace("|", "\\|")));
      }
    }
    return table.toString();
  }

  private static Map<String, RecordComponent> services() {
    return ExternalEndpoints.keys(TestEndpoints.class).entrySet().stream()
        .filter(e -> TestEndpoints.Service.class.isAssignableFrom(e.getValue().getType()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static void checkGenerated(Path relative, String expected) throws IOException {
    Path file = repoRoot().resolve(relative);
    if (UPDATE) {
      Files.writeString(file, expected);
      return;
    }
    Assertions.assertEquals(
        expected,
        Files.exists(file) ? Files.readString(file) : "",
        relative + " is stale; rerun with -Pgravitino.test.updateGenerated=true to rewrite it");
  }

  // Gradle runs tests in the project directory; the files live under the repository root.
  private static Path repoRoot() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null && !Files.exists(dir.resolve("settings.gradle.kts"))) {
      dir = dir.getParent();
    }
    return requireNonNull(dir, "no settings.gradle.kts above the working directory");
  }

  private static final String LICENSE =
      "Licensed to the Apache Software Foundation (ASF) under one or more contributor license"
          + " agreements.  See the NOTICE file distributed with this work for additional"
          + " information regarding copyright ownership.  The ASF licenses this file to you under"
          + " the Apache License, Version 2.0 (the \"License\"); you may not use this file except"
          + " in compliance with the License.  You may obtain a copy of the License at"
          + " http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or"
          + " agreed to in writing, software distributed under the License is distributed on an"
          + " \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or"
          + " implied.  See the License for the specific language governing permissions and"
          + " limitations under the License. Generated from TestEndpoints by"
          + " TestExternalEndpoints; do not edit.";

  // The external constructor is private to the builder; the test reaches it the way the builder
  // does.
  private static final class ExternalHive extends HiveContainer {
    ExternalHive(TestEndpoints.Hive endpoints) {
      super(endpoints);
    }
  }
}
