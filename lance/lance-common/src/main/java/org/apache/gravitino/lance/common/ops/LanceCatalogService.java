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
package org.apache.gravitino.lance.common.ops;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin placeholder that will later bridge Lance catalog metadata into Gravitino.
 *
 * <p>The current implementation keeps an in-memory catalog view so the REST surface mirrors the
 * Iceberg catalog experience while the Lance integration is built out for real.
 */
public class LanceCatalogService implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogService.class);

  private final LanceConfig config;
  private final ConcurrentMap<String, NamespaceState> namespaces;

  public LanceCatalogService(LanceConfig config) {
    this.config = config;
    this.namespaces = new ConcurrentHashMap<>();
    seedSampleMetadata();
  }

  public String catalogName() {
    return config.getCatalogName();
  }

  public boolean namespaceExists(String namespace) {
    return namespaces.containsKey(namespace);
  }

  public Map<String, Map<String, String>> listNamespaces() {
    Map<String, Map<String, String>> result = new ConcurrentHashMap<>();
    namespaces.forEach(
        (name, state) ->
            result.put(
                name, Collections.unmodifiableMap(new ConcurrentHashMap<>(state.properties))));
    return Map.copyOf(result);
  }

  public List<String> listNamespaceNames() {
    return namespaces.keySet().stream()
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.toUnmodifiableList());
  }

  public boolean createNamespace(String namespace) {
    if (StringUtils.isBlank(namespace)) {
      throw new IllegalArgumentException("Namespace must be non-empty");
    }
    NamespaceState state = new NamespaceState(Collections.emptyMap());
    NamespaceState existing = namespaces.putIfAbsent(namespace, state);
    if (existing == null) {
      LOG.info("Created Lance namespace {}", namespace);
      return true;
    }
    return false;
  }

  public boolean dropNamespace(String namespace) {
    NamespaceState state = namespaces.get(namespace);
    if (state == null) {
      return false;
    }
    if (!state.tables.isEmpty()) {
      LOG.info("Refusing to drop Lance namespace {} because it still owns tables", namespace);
      return false;
    }
    boolean removed = namespaces.remove(namespace, state);
    if (removed) {
      LOG.info("Dropped Lance namespace {}", namespace);
    }
    return removed;
  }

  public List<String> listTables(String namespace) {
    NamespaceState state = namespaces.get(namespace);
    if (state == null) {
      throw new IllegalArgumentException("Unknown namespace: " + namespace);
    }
    return state.tables.keySet().stream()
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<Map<String, Object>> loadTable(String namespace, String table) {
    NamespaceState state = namespaces.get(namespace);
    if (state == null) {
      return Optional.empty();
    }
    LanceTableEntry tableEntry = state.tables.get(table);
    if (tableEntry == null) {
      return Optional.empty();
    }
    return Optional.of(tableEntry.describe());
  }

  @Override
  public void close() {
    namespaces.clear();
  }

  private void seedSampleMetadata() {
    NamespaceState defaultNamespace =
        namespaces.computeIfAbsent("default", key -> new NamespaceState(Collections.emptyMap()));
    defaultNamespace.tables.put(
        "sample_table",
        new LanceTableEntry(
            "sample_table",
            "default",
            ImmutableMap.of(
                "format", "lance",
                "uri", "file:///tmp/sample_table.lance",
                "summary", "Placeholder Lance table metadata")));
  }

  private static final class NamespaceState {
    private final Map<String, String> properties;
    private final ConcurrentMap<String, LanceTableEntry> tables;

    NamespaceState(Map<String, String> properties) {
      this.properties = new ConcurrentHashMap<>(properties);
      this.tables = new ConcurrentHashMap<>();
    }
  }

  private static final class LanceTableEntry {
    private final String name;
    private final String namespace;
    private final Map<String, Object> metadata;

    LanceTableEntry(String name, String namespace, Map<String, Object> metadata) {
      this.name = name;
      this.namespace = namespace;
      this.metadata = new ConcurrentHashMap<>(metadata);
    }

    Map<String, Object> describe() {
      Map<String, Object> result = new ConcurrentHashMap<>(metadata);
      result.put("name", name);
      result.put("namespace", namespace);
      return Collections.unmodifiableMap(result);
    }
  }
}
