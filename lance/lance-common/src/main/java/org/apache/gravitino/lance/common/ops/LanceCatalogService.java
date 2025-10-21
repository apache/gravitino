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
import java.util.NoSuchElementException;
import java.util.Objects;
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

  public NamespaceListingResult listChildNamespaces(
      String parentId, String delimiter, String pageToken, Integer limit) {
    String normalizedParent = StringUtils.trimToEmpty(parentId);
    String effectiveDelimiter = StringUtils.isBlank(delimiter) ? "$" : delimiter;

    List<String> sortedNamespaces = listNamespaceNames();
    List<String> filtered = filterChildren(sortedNamespaces, normalizedParent, effectiveDelimiter);

    int startingOffset = parsePageToken(pageToken, filtered.size());
    int pageLimit = limit == null ? filtered.size() : validatePositiveLimit(limit, filtered.size());
    int endIndex = Math.min(filtered.size(), startingOffset + pageLimit);

    List<String> page = filtered.subList(startingOffset, endIndex);
    String nextToken = endIndex < filtered.size() ? String.valueOf(endIndex) : null;
    return new NamespaceListingResult(normalizedParent, effectiveDelimiter, page, nextToken);
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

  public TableListingResult listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    String normalizedNamespace = StringUtils.trimToEmpty(namespaceId);
    if (StringUtils.isBlank(normalizedNamespace)) {
      throw new IllegalArgumentException("Namespace id must be provided");
    }

    String effectiveDelimiter = StringUtils.isBlank(delimiter) ? "$" : delimiter;

    NamespaceState state = namespaces.get(normalizedNamespace);
    if (state == null) {
      throw new NoSuchElementException("Unknown namespace: " + normalizedNamespace);
    }

    List<String> sortedTables =
        state.tables.keySet().stream()
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());

    int startingOffset = parsePageToken(pageToken, sortedTables.size());
    int pageLimit =
        limit == null ? sortedTables.size() : validatePositiveLimit(limit, sortedTables.size());
    int endIndex = Math.min(sortedTables.size(), startingOffset + pageLimit);

    List<String> page = sortedTables.subList(startingOffset, endIndex);
    String nextToken = endIndex < sortedTables.size() ? String.valueOf(endIndex) : null;

    return new TableListingResult(normalizedNamespace, effectiveDelimiter, page, nextToken);
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

  private List<String> filterChildren(List<String> namespaces, String parentId, String delimiter) {
    boolean rootRequest = StringUtils.isBlank(parentId) || "root".equalsIgnoreCase(parentId);
    if (rootRequest) {
      return namespaces;
    }

    String parentPrefix = parentId + delimiter;
    return namespaces.stream()
        .filter(ns -> ns.startsWith(parentPrefix))
        .map(
            ns -> {
              String remainder = ns.substring(parentPrefix.length());
              int nextDelimiter = remainder.indexOf(delimiter);
              if (nextDelimiter >= 0) {
                return remainder.substring(0, nextDelimiter);
              }
              return remainder;
            })
        .filter(child -> !child.isEmpty())
        .distinct()
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.toUnmodifiableList());
  }

  private int parsePageToken(String pageToken, int size) {
    if (StringUtils.isBlank(pageToken)) {
      return 0;
    }
    try {
      int parsed = Integer.parseInt(pageToken);
      if (parsed < 0 || parsed > size) {
        throw new IllegalArgumentException("Invalid page_token value");
      }
      return parsed;
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Invalid page_token value", nfe);
    }
  }

  private int validatePositiveLimit(int limit, int size) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be greater than 0");
    }
    return Math.min(limit, Math.max(size, 0));
  }

  public static final class NamespaceListingResult {
    private final String parentId;
    private final String delimiter;
    private final List<String> namespaces;
    private final String nextPageToken;

    NamespaceListingResult(
        String parentId, String delimiter, List<String> namespaces, String nextPageToken) {
      this.parentId = parentId;
      this.delimiter = delimiter;
      this.namespaces = List.copyOf(namespaces);
      this.nextPageToken = nextPageToken;
    }

    public String getParentId() {
      return parentId;
    }

    public String getDelimiter() {
      return delimiter;
    }

    public List<String> getNamespaces() {
      return namespaces;
    }

    public Optional<String> getNextPageToken() {
      return Optional.ofNullable(nextPageToken);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NamespaceListingResult)) {
        return false;
      }
      NamespaceListingResult that = (NamespaceListingResult) o;
      return Objects.equals(parentId, that.parentId)
          && Objects.equals(delimiter, that.delimiter)
          && Objects.equals(namespaces, that.namespaces)
          && Objects.equals(nextPageToken, that.nextPageToken);
    }

    @Override
    public int hashCode() {
      return Objects.hash(parentId, delimiter, namespaces, nextPageToken);
    }
  }

  public static final class TableListingResult {
    private final String namespaceId;
    private final String delimiter;
    private final List<String> tables;
    private final String nextPageToken;

    TableListingResult(
        String namespaceId, String delimiter, List<String> tables, String nextPageToken) {
      this.namespaceId = namespaceId;
      this.delimiter = delimiter;
      this.tables = List.copyOf(tables);
      this.nextPageToken = nextPageToken;
    }

    public String getNamespaceId() {
      return namespaceId;
    }

    public String getDelimiter() {
      return delimiter;
    }

    public List<String> getTables() {
      return tables;
    }

    public Optional<String> getNextPageToken() {
      return Optional.ofNullable(nextPageToken);
    }
  }
}
