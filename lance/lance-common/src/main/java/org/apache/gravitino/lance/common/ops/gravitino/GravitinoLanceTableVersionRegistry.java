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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.config.LanceConfig.METALAKE_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.lance.common.model.BatchCreateTableVersionsRequest;
import org.apache.gravitino.lance.common.model.BatchCreateTableVersionsResponse;
import org.apache.gravitino.lance.common.model.BatchDeleteTableVersionsRequest;
import org.apache.gravitino.lance.common.model.BatchDeleteTableVersionsResponse;
import org.apache.gravitino.lance.common.model.CreateTableVersionEntry;
import org.apache.gravitino.lance.common.model.CreateTableVersionRequest;
import org.apache.gravitino.lance.common.model.DescribeTableVersionRequest;
import org.apache.gravitino.lance.common.model.ListTableVersionsResponse;
import org.apache.gravitino.lance.common.model.TableVersionInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.mapper.LanceTableVersionMapper;
import org.apache.gravitino.storage.relational.po.LanceTableVersionPO;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.lance.namespace.errors.ConcurrentModificationException;
import org.lance.namespace.errors.TableVersionNotFoundException;

class GravitinoLanceTableVersionRegistry {
  private static final TypeReference<Map<String, String>> STRING_MAP_TYPE =
      new TypeReference<Map<String, String>>() {};
  private static final int DEFAULT_LIST_LIMIT = 100;
  private static final int MAX_LIST_LIMIT = 1000;

  private final GravitinoLanceNamespaceWrapper namespaceWrapper;

  GravitinoLanceTableVersionRegistry(GravitinoLanceNamespaceWrapper namespaceWrapper) {
    this.namespaceWrapper = namespaceWrapper;
  }

  TableVersionInfo createTableVersion(
      String tableId, String delimiter, CreateTableVersionRequest request) {
    ResolvedTable resolvedTable = resolvePathStyleTable(tableId, delimiter);
    LanceTableVersionPO po =
        newVersionPO(
            resolvedTable,
            request.getVersion(),
            request.getManifestPath(),
            request.getManifestSize(),
            request.getETag(),
            request.getNamingScheme(),
            request.getMetadata(),
            request.getContext());

    try {
      SessionUtils.doWithCommit(
          LanceTableVersionMapper.class, mapper -> mapper.insertTableVersion(po));
    } catch (RuntimeException e) {
      throw toConflictIfDuplicate(tableId, request.getVersion(), e);
    }

    return toTableVersionInfo(po);
  }

  ListTableVersionsResponse listTableVersions(
      String tableId, String delimiter, Integer limit, boolean descending, String pageToken) {
    ResolvedTable resolvedTable = resolvePathStyleTable(tableId, delimiter);
    int validatedLimit = validateAndNormalizeLimit(limit);
    Long startVersionExclusive = parsePageToken(pageToken);

    List<LanceTableVersionPO> versionPOs =
        SessionUtils.doWithCommitAndFetchResult(
            LanceTableVersionMapper.class,
            mapper ->
                mapper.listTableVersions(
                    resolvedTable.tableEntityId, validatedLimit, startVersionExclusive, descending));

    ListTableVersionsResponse response = new ListTableVersionsResponse();
    response.setVersions(
        versionPOs.stream()
            .map(GravitinoLanceTableVersionRegistry::toTableVersionInfo)
            .collect(Collectors.toList()));
    if (!versionPOs.isEmpty() && versionPOs.size() == validatedLimit) {
      response.setPageToken(String.valueOf(versionPOs.get(versionPOs.size() - 1).getVersion()));
    }
    return response;
  }

  TableVersionInfo describeTableVersion(
      String tableId, String delimiter, DescribeTableVersionRequest request) {
    ResolvedTable resolvedTable = resolvePathStyleTable(tableId, delimiter);
    LanceTableVersionPO po =
        SessionUtils.doWithCommitAndFetchResult(
            LanceTableVersionMapper.class,
            mapper -> mapper.selectTableVersion(resolvedTable.tableEntityId, request.getVersion()));
    if (po == null) {
      throw new TableVersionNotFoundException(
          "Table version not found: " + tableId + "@" + request.getVersion(),
          CommonUtil.formatCurrentStackTrace(),
          tableId);
    }
    return toTableVersionInfo(po);
  }

  BatchCreateTableVersionsResponse batchCreateTableVersions(
      BatchCreateTableVersionsRequest request) {
    List<LanceTableVersionPO> versionPOs = new ArrayList<>(request.getEntries().size());
    for (CreateTableVersionEntry entry : request.getEntries()) {
      ResolvedTable resolvedTable = resolveListStyleTable(entry.getId());
      versionPOs.add(
          newVersionPO(
              resolvedTable,
              entry.getVersion(),
              entry.getManifestPath(),
              entry.getManifestSize(),
              entry.getETag(),
              entry.getNamingScheme(),
              entry.getMetadata(),
              entry.getContext()));
    }

    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  LanceTableVersionMapper.class,
                  mapper -> mapper.batchInsertTableVersions(versionPOs)));
    } catch (RuntimeException e) {
      throw toConflictIfDuplicate("batch-create", null, e);
    }

    BatchCreateTableVersionsResponse response = new BatchCreateTableVersionsResponse();
    response.setVersions(
        versionPOs.stream()
            .map(GravitinoLanceTableVersionRegistry::toTableVersionInfo)
            .collect(Collectors.toList()));
    return response;
  }

  BatchDeleteTableVersionsResponse batchDeleteTableVersions(
      String tableId, String delimiter, BatchDeleteTableVersionsRequest request) {
    ResolvedTable resolvedTable = resolvePathStyleTable(tableId, delimiter);
    long deletedAt = System.currentTimeMillis();
    Integer deletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            LanceTableVersionMapper.class,
            mapper ->
                mapper.softDeleteTableVersions(
                    resolvedTable.tableEntityId, request.getVersions(), deletedAt));
    BatchDeleteTableVersionsResponse response = new BatchDeleteTableVersionsResponse();
    response.setDeletedCount(Optional.ofNullable(deletedCount).orElse(0));
    response.setVersions(request.getVersions());
    return response;
  }

  private ResolvedTable resolvePathStyleTable(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected a 3-level table id but got: %s", tableId);
    return resolveTable(nsId.levelAtListPos(0), nsId.levelAtListPos(1), nsId.levelAtListPos(2));
  }

  private ResolvedTable resolveListStyleTable(List<String> id) {
    Preconditions.checkArgument(id != null && id.size() == 3, "Table id must have 3 levels.");
    return resolveTable(id.get(0), id.get(1), id.get(2));
  }

  private ResolvedTable resolveTable(String catalogName, String schemaName, String tableName) {
    namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);

    String metalakeName = namespaceWrapper.config().get(METALAKE_NAME);
    NameIdentifier schemaIdentifier = NameIdentifier.of(metalakeName, catalogName, schemaName);
    NamespacedEntityId schemaIds =
        EntityIdService.getEntityIds(schemaIdentifier, Entity.EntityType.SCHEMA);
    Long tableEntityId =
        TableMetaService.getInstance().getTableIdBySchemaIdAndName(schemaIds.entityId(), tableName);

    return new ResolvedTable(
        metalakeName,
        catalogName,
        schemaName,
        tableName,
        schemaIds.namespaceIds()[0],
        schemaIds.namespaceIds()[1],
        schemaIds.entityId(),
        tableEntityId);
  }

  private static LanceTableVersionPO newVersionPO(
      ResolvedTable table,
      Long version,
      String manifestPath,
      Long manifestSize,
      String eTag,
      String namingScheme,
      Map<String, String> metadata,
      Map<String, String> context) {
    long now = System.currentTimeMillis();
    return LanceTableVersionPO.builder()
        .withMetalakeId(table.metalakeId)
        .withCatalogId(table.catalogId)
        .withSchemaId(table.schemaId)
        .withTableId(table.tableEntityId)
        .withVersion(version)
        .withManifestPath(manifestPath)
        .withManifestSize(manifestSize)
        .withETag(eTag)
        .withNamingScheme(namingScheme)
        .withMetadata(toJson(metadata))
        .withContext(toJson(context))
        .withCreatedAt(now)
        .withCreatedBy(extractCreatedBy(metadata, context))
        .withRequestId(extractRequestId(metadata, context))
        .withDeletedAt(0L)
        .build();
  }

  private static TableVersionInfo toTableVersionInfo(LanceTableVersionPO po) {
    TableVersionInfo info = new TableVersionInfo();
    info.setVersion(po.getVersion());
    info.setTimestamp(OffsetDateTime.ofInstant(Instant.ofEpochMilli(po.getCreatedAt()), ZoneOffset.UTC));
    info.setManifestPath(po.getManifestPath());
    info.setManifestSize(po.getManifestSize());
    info.setETag(po.getETag());
    info.setNamingScheme(po.getNamingScheme());
    info.setMetadata(fromJson(po.getMetadata()));
    info.setContext(fromJson(po.getContext()));
    info.setCreatedBy(po.getCreatedBy());
    info.setRequestId(po.getRequestId());
    return info;
  }

  private static int validateAndNormalizeLimit(Integer limit) {
    int validatedLimit = limit == null ? DEFAULT_LIST_LIMIT : limit;
    Preconditions.checkArgument(validatedLimit > 0, "Limit must be positive.");
    Preconditions.checkArgument(validatedLimit <= MAX_LIST_LIMIT, "Limit must be <= 1000.");
    return validatedLimit;
  }

  private static Long parsePageToken(String pageToken) {
    if (StringUtils.isBlank(pageToken)) {
      return null;
    }
    try {
      long token = Long.parseLong(pageToken);
      Preconditions.checkArgument(token > 0, "page_token must be positive.");
      return token;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("page_token must be a valid version number.", e);
    }
  }

  private static String toJson(Map<String, String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(values);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize map payload.", e);
    }
  }

  private static Map<String, String> fromJson(String rawJson) {
    if (StringUtils.isBlank(rawJson)) {
      return Collections.emptyMap();
    }
    try {
      return JsonUtils.anyFieldMapper().readValue(rawJson, STRING_MAP_TYPE);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to deserialize stored map payload.", e);
    }
  }

  private static String extractCreatedBy(
      Map<String, String> metadata, Map<String, String> context) {
    return firstNonBlank(
        context == null ? null : context.get("actor"),
        context == null ? null : context.get("created_by"),
        metadata == null ? null : metadata.get("actor"));
  }

  private static String extractRequestId(
      Map<String, String> metadata, Map<String, String> context) {
    return firstNonBlank(
        context == null ? null : context.get("request_id"),
        context == null ? null : context.get("trace_id"),
        metadata == null ? null : metadata.get("request_id"),
        metadata == null ? null : metadata.get("trace_id"));
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        return value;
      }
    }
    return null;
  }

  private static RuntimeException toConflictIfDuplicate(
      String instance, Long version, RuntimeException runtimeException) {
    if (!isDuplicateKey(runtimeException)) {
      return runtimeException;
    }

    String message =
        version == null
            ? "One or more table versions already exist."
            : "Table version already exists: " + instance + "@" + version;
    return new ConcurrentModificationException(
        message, CommonUtil.formatCurrentStackTrace(), instance);
  }

  private static boolean isDuplicateKey(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof SQLException) {
        SQLException sqlException = (SQLException) current;
        if ("23505".equals(sqlException.getSQLState())
            || sqlException.getErrorCode() == 23505
            || sqlException.getErrorCode() == 1062) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  private static class ResolvedTable {
    private final String metalakeName;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final long metalakeId;
    private final long catalogId;
    private final long schemaId;
    private final long tableEntityId;

    private ResolvedTable(
        String metalakeName,
        String catalogName,
        String schemaName,
        String tableName,
        long metalakeId,
        long catalogId,
        long schemaId,
        long tableEntityId) {
      this.metalakeName = metalakeName;
      this.catalogName = catalogName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.metalakeId = metalakeId;
      this.catalogId = catalogId;
      this.schemaId = schemaId;
      this.tableEntityId = tableEntityId;
    }
  }
}
