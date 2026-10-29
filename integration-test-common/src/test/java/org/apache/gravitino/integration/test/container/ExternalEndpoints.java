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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Reads the endpoints of services that run outside this JVM's Testcontainers. A container built
 * with them is "external": not started, not stopped, and reachable only through its accessors.
 *
 * <p>The YAML file that {@code gravitino.test.endpoints.file} names is bound to {@link
 * TestEndpoints}. Flat keys override it: {@code gravitino.test.<service>[.<instance>].<key>} as a
 * system property (Gradle forwards {@code -P}), then {@code GRAVITINO_TEST_<SERVICE>_..._<KEY>} in
 * the environment. A list value is comma-separated, a map value {@code k1=v1,k2=v2}. A key that
 * {@link TestEndpoints} does not declare fails the run, with the closest declared key.
 */
public final class ExternalEndpoints {
  static final String PREFIX = "gravitino.test.";
  static final String FILE_KEY = PREFIX + "endpoints.file";

  private static final ObjectMapper YAML =
      new ObjectMapper(new YAMLFactory()).enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  // Read once per JVM, at the first container built.
  private static final Supplier<TestEndpoints> RUN =
      Suppliers.memoize(() -> read(systemProperties(), System.getenv()));

  private ExternalEndpoints() {}

  /**
   * Returns the endpoints of one instance of a service when the run declares the service external,
   * after checking that the instance has every required key. An instance listed in the service's
   * instances map uses that block whole; any other instance, and the empty name, use the service's
   * own keys.
   *
   * @param service the service, as in the file: {@code hive}, {@code kafka}, {@code mysql}
   * @param instance the instance name; empty for the service's own block
   * @param <T> the service's record in {@link TestEndpoints}
   * @return the endpoints, or empty when the service runs as a managed container
   * @throws IllegalStateException if the file or an override has an undeclared key, or the service
   *     is external and a required key is missing
   */
  public static <T extends TestEndpoints.Service> Optional<T> forService(
      String service, String instance) {
    return forService(RUN.get(), service, instance);
  }

  /**
   * Returns whether every service is external, so the run needs no Docker at all.
   *
   * @return whether {@code containers: external}
   */
  public static boolean allExternal() {
    return RUN.get().containers() == TestEndpoints.Mode.EXTERNAL;
  }

  @SuppressWarnings("unchecked")
  static <T extends TestEndpoints.Service> Optional<T> forService(
      TestEndpoints endpoints, String service, String instance) {
    requireNonNull(instance, "instance is null");
    RecordComponent component = keys(TestEndpoints.class).get(service);
    if (component == null || !TestEndpoints.Service.class.isAssignableFrom(component.getType())) {
      throw new IllegalArgumentException("No service " + service + " in TestEndpoints");
    }
    TestEndpoints.Service block = (TestEndpoints.Service) value(component, endpoints);
    boolean external =
        endpoints.containers() == TestEndpoints.Mode.EXTERNAL
            || (block != null && Boolean.TRUE.equals(block.external()));
    if (!external) {
      return Optional.empty();
    }
    String path = service;
    if (block != null && block.instances().containsKey(instance)) {
      block = block.instances().get(instance);
      path = service + ".instances." + instance;
    }
    checkRequired(component.getType(), block, path);
    return Optional.of((T) block);
  }

  /**
   * Reads the file and the overrides of a run.
   *
   * @param properties the system properties
   * @param env the environment
   * @return the endpoints
   * @throws IllegalStateException if the file cannot be read or has, or an override sets, an
   *     undeclared key
   */
  static TestEndpoints read(Map<String, String> properties, Map<String, String> env) {
    String file = properties.getOrDefault(FILE_KEY, env.get(envName(FILE_KEY)));
    String origin = file == null ? "no file" : "the file " + file;
    ObjectNode tree = file == null ? JsonNodeFactory.instance.objectNode() : readFile(file);
    // Environment first, so a property set to the same key wins.
    env.forEach((name, value) -> override(tree, fromEnv(name), value));
    properties.forEach((name, value) -> override(tree, fromProperty(name), value));
    try {
      return YAML.treeToValue(tree, TestEndpoints.class);
    } catch (UnrecognizedPropertyException e) {
      throw new IllegalStateException(unknownKey(e, origin), e);
    } catch (JsonMappingException e) {
      throw new IllegalStateException(
          "Invalid test endpoints ("
              + origin
              + ", -Pgravitino.test.*, GRAVITINO_TEST_*): "
              + e.getOriginalMessage()
              + " at "
              + path(e),
          e);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot bind the test endpoints", e);
    }
  }

  private static ObjectNode readFile(String file) {
    JsonNode read;
    try {
      read = YAML.readTree(Path.of(file).toFile());
    } catch (IOException e) {
      throw new IllegalStateException("Cannot read the endpoints file " + file, e);
    }
    if (read == null || read.isMissingNode() || read.isNull()) {
      return JsonNodeFactory.instance.objectNode();
    }
    if (!read.isObject()) {
      throw new IllegalStateException("The endpoints file " + file + " is not a YAML mapping");
    }
    return (ObjectNode) read;
  }

  /**
   * Returns the keys of a record by their name in the file, in declaration order.
   *
   * @param type a record in {@link TestEndpoints}
   * @return the components, by key
   */
  static Map<String, RecordComponent> keys(Class<?> type) {
    return Arrays.stream(type.getRecordComponents())
        .collect(
            Collectors.toMap(
                // Jackson's annotations reach a component's accessor, not the component.
                c -> c.getAccessor().getAnnotation(JsonProperty.class).value(),
                c -> c,
                (a, b) -> a,
                LinkedHashMap::new));
  }

  /**
   * Returns the environment variable that sets a property.
   *
   * @param property a property, such as {@code gravitino.test.kafka.bootstrap-servers}
   * @return its name in upper case with {@code .} and {@code -} as {@code _}
   */
  static String envName(String property) {
    return property.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_');
  }

  // The path in the file a property sets, or null when it is no endpoint key:
  // gravitino.test.<service>[.<instance>].<key> or gravitino.test.containers.
  @Nullable
  private static List<String> fromProperty(String name) {
    if (!name.startsWith(PREFIX)) {
      return null;
    }
    List<String> parts = List.of(name.substring(PREFIX.length()).split("\\.", 3));
    RecordComponent top = keys(TestEndpoints.class).get(parts.get(0));
    if (top == null || (parts.size() == 1) == isService(top)) {
      return null;
    }
    return parts.size() == 3
        ? List.of(parts.get(0), "instances", parts.get(1), parts.get(2))
        : parts;
  }

  // The same for an environment variable, whose "_" may stand for ".", "-" or "_": the rest after
  // GRAVITINO_TEST_<SERVICE>_ is a declared key, or an instance name and a declared key.
  @Nullable
  private static List<String> fromEnv(String name) {
    for (Map.Entry<String, RecordComponent> top : keys(TestEndpoints.class).entrySet()) {
      String prefix = envName(PREFIX + top.getKey());
      if (!isService(top.getValue())) {
        if (name.equals(prefix)) {
          return List.of(top.getKey());
        }
        continue;
      }
      if (!name.startsWith(prefix + "_")) {
        continue;
      }
      String rest = name.substring(prefix.length() + 1);
      Iterable<String> declared = keys(top.getValue().getType()).keySet();
      for (String key : declared) {
        if (rest.equals(envName(key))) {
          return List.of(top.getKey(), key);
        }
      }
      for (String key : declared) {
        String keyEnv = envName(key);
        if (rest.endsWith("_" + keyEnv)) {
          String instance = rest.substring(0, rest.length() - keyEnv.length() - 1);
          return List.of(top.getKey(), "instances", instance.toLowerCase(Locale.ROOT), key);
        }
      }
      // Undeclared: bind it as written, so the unknown key fails with a suggestion.
      return List.of(top.getKey(), rest.toLowerCase(Locale.ROOT).replace('_', '-'));
    }
    return null;
  }

  // Sets a flat value at a path of the tree, as a list or map when the declared key is one.
  private static void override(ObjectNode tree, @Nullable List<String> path, String value) {
    if (path == null) {
      return;
    }
    ObjectNode node = tree;
    Class<?> type = TestEndpoints.class;
    for (int i = 0; i < path.size() - 1; i++) {
      String part = path.get(i);
      node = child(node, part);
      if (part.equals("instances")) {
        // An instance holds the keys of its service.
        node = child(node, path.get(++i));
      } else if (type != null && type.isRecord()) {
        RecordComponent component = keys(type).get(part);
        type = component == null ? null : component.getType();
      }
    }
    String key = path.get(path.size() - 1);
    RecordComponent component = type != null && type.isRecord() ? keys(type).get(key) : null;
    Class<?> valueType = component == null ? String.class : component.getType();
    if (List.class.isAssignableFrom(valueType)) {
      ArrayNode list = node.putArray(key);
      split(value, ",").forEach(list::add);
    } else if (Map.class.isAssignableFrom(valueType)) {
      ObjectNode map = node.putObject(key);
      for (String entry : split(value, ",")) {
        int eq = entry.indexOf('=');
        if (eq <= 0) {
          throw new IllegalStateException(
              String.format("%s: entry %s is not key=value", String.join(".", path), entry));
        }
        map.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
      }
    } else {
      node.put(key, value);
    }
  }

  private static ObjectNode child(ObjectNode node, String name) {
    JsonNode child = node.get(name);
    return child instanceof ObjectNode ? (ObjectNode) child : node.putObject(name);
  }

  private static List<String> split(String value, String separator) {
    return Arrays.stream(value.split(separator))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private static boolean isService(RecordComponent component) {
    return TestEndpoints.Service.class.isAssignableFrom(component.getType());
  }

  private static void checkRequired(
      Class<?> type, @Nullable TestEndpoints.Service block, String path) {
    List<String> missing =
        keys(type).entrySet().stream()
            .filter(key -> key.getValue().isAnnotationPresent(TestEndpoints.Required.class))
            .filter(key -> block == null || value(key.getValue(), block) == null)
            .map(key -> howToSet(path, key.getKey()))
            .collect(Collectors.toList());
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "External %s needs %d more key(s):%n  %s",
              path, missing.size(), String.join(String.format("%n  "), missing)));
    }
  }

  private static String howToSet(String path, String key) {
    String property = PREFIX + path.replace(".instances.", ".") + "." + key;
    return String.format(
        "%s.%s: set it in the file %s names, -P%s=..., or %s",
        path, key, FILE_KEY, property, envName(property));
  }

  private static String unknownKey(UnrecognizedPropertyException e, String origin) {
    String name = e.getPropertyName();
    List<String> known =
        Optional.ofNullable(e.getKnownPropertyIds()).orElse(List.of()).stream()
            .map(Object::toString)
            .sorted()
            .collect(Collectors.toList());
    String closest =
        known.stream().min(Comparator.comparingInt(k -> distance(k, name))).orElse("?");
    return String.format(
        "Unknown test endpoints key %s (%s, -Pgravitino.test.*, GRAVITINO_TEST_*); did you mean"
            + " %s? Valid keys there: %s",
        path(e), origin, closest, String.join(", ", known));
  }

  private static String path(JsonMappingException e) {
    return e.getPath().stream()
        .map(ref -> ref.getFieldName() != null ? ref.getFieldName() : "[" + ref.getIndex() + "]")
        .collect(Collectors.joining("."));
  }

  @Nullable
  private static Object value(RecordComponent component, Object record) {
    try {
      return component.getAccessor().invoke(record);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Cannot read " + component, e);
    }
  }

  private static Map<String, String> systemProperties() {
    Map<String, String> properties = new LinkedHashMap<>();
    System.getProperties()
        .stringPropertyNames()
        .forEach(name -> properties.put(name, System.getProperty(name)));
    return properties;
  }

  // Levenshtein distance, for suggesting the declared key a typo meant.
  static int distance(String a, String b) {
    int[] previous = new int[b.length() + 1];
    int[] current = new int[b.length() + 1];
    for (int j = 0; j <= b.length(); j++) {
      previous[j] = j;
    }
    for (int i = 1; i <= a.length(); i++) {
      current[0] = i;
      for (int j = 1; j <= b.length(); j++) {
        int substitution = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
        current[j] =
            Math.min(Math.min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + substitution);
      }
      int[] swap = previous;
      previous = current;
      current = swap;
    }
    return previous[b.length()];
  }
}
