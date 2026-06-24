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
package org.apache.gravitino.lance.common.ops.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Namespace operations for the Hive Lance backend. A namespace is a single Hive database; the root
 * (zero levels) lists all databases.
 */
public class HiveLanceNamespaceOperations implements LanceNamespaceOperations {

  private static final Logger LOG = LoggerFactory.getLogger(HiveLanceNamespaceOperations.class);

  private static final String MODE_CREATE = "create";
  private static final String MODE_EXIST_OK = "exist_ok";
  private static final String MODE_EXISTOK = "existok";
  private static final String MODE_OVERWRITE = "overwrite";
  private static final String MODE_SKIP = "skip";
  private static final String BEHAVIOR_CASCADE = "cascade";

  private final HiveClientPool clientPool;
  private final String root;

  /**
   * Create namespace operations bound to the given wrapper.
   *
   * @param wrapper the owning namespace wrapper
   */
  public HiveLanceNamespaceOperations(HiveLanceNamespaceWrapper wrapper) {
    this(wrapper.getClientPool(), wrapper.getRoot());
  }

  @VisibleForTesting
  HiveLanceNamespaceOperations(HiveClientPool clientPool, String root) {
    this.clientPool = clientPool;
    this.root = root;
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() <= 1, "Expect a 1-level namespace but get %s", namespaceId);

    List<String> namespaces;
    if (nsId.levels() == 0) {
      namespaces = doListDatabases();
    } else {
      namespaces = Lists.newArrayList();
    }

    Collections.sort(namespaces);
    PageUtil.Page page =
        PageUtil.splitPage(namespaces, pageToken, PageUtil.normalizePageSize(limit));

    ListNamespacesResponse response = new ListNamespacesResponse();
    response.setNamespaces(Sets.newHashSet(page.items()));
    response.setPageToken(page.nextPageToken());
    return response;
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(String namespaceId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 1, "Expect a 1-level namespace but get %s", namespaceId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    Database database = HiveUtil.getDatabaseOrNull(clientPool, db);
    if (database == null) {
      throw new NamespaceNotFoundException(
          String.format("Namespace does not exist: %s", db),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          db);
    }

    DescribeNamespaceResponse response = new DescribeNamespaceResponse();
    response.setProperties(collectDatabaseProperties(database));
    return response;
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      String namespaceId, String delimiter, String mode, Map<String, String> properties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 1, "Expect a 1-level namespace but get %s", namespaceId);

    String normalizedMode = mode != null ? mode.toLowerCase(Locale.ROOT) : MODE_CREATE;
    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    createDatabase(db, normalizedMode, properties);

    CreateNamespaceResponse response = new CreateNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public DropNamespaceResponse dropNamespace(
      String namespaceId, String delimiter, String mode, String behavior) {
    if (BEHAVIOR_CASCADE.equalsIgnoreCase(behavior)) {
      throw new InvalidInputException("Cascade behavior is not supported for the Hive backend");
    }

    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 1, "Expect a 1-level namespace but get %s", namespaceId);

    String normalizedMode = mode != null ? mode.toLowerCase(Locale.ROOT) : "fail";
    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    Map<String, String> properties = doDropDatabase(db, normalizedMode);

    DropNamespaceResponse response = new DropNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public void namespaceExists(String namespaceId, String delimiter) throws LanceNamespaceException {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 1, "Expect a 1-level namespace but get %s", namespaceId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    Database database = HiveUtil.getDatabaseOrNull(clientPool, db);
    if (database == null) {
      throw new NamespaceNotFoundException(
          String.format("Namespace does not exist: %s", db),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          db);
    }
  }

  @Override
  public ListTablesResponse listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 1, "Expect a 1-level namespace but get %s", namespaceId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    List<String> tables = doListTables(db);

    Collections.sort(tables);
    PageUtil.Page page = PageUtil.splitPage(tables, pageToken, PageUtil.normalizePageSize(limit));

    ListTablesResponse response = new ListTablesResponse();
    response.setTables(Sets.newHashSet(page.items()));
    response.setPageToken(page.nextPageToken());
    return response;
  }

  private List<String> doListDatabases() {
    try {
      return clientPool.run(IMetaStoreClient::getAllDatabases);
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          "Failed to list namespaces: " + errorMessage(e),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          "");
    }
  }

  private void createDatabase(String dbName, String mode, Map<String, String> properties) {
    try {
      Database oldDb = HiveUtil.getDatabaseOrNull(clientPool, dbName);
      if (oldDb != null) {
        if (MODE_CREATE.equals(mode)) {
          throw new NamespaceAlreadyExistsException(
              String.format("Database %s already exist", dbName),
              HiveErrorType.DATABASE_ALREADY_EXIST.getType(),
              "");
        } else if (MODE_EXIST_OK.equals(mode) || MODE_EXISTOK.equals(mode)) {
          return;
        } else if (MODE_OVERWRITE.equals(mode)) {
          clientPool.run(
              client -> {
                client.dropDatabase(dbName, false, true, false);
                return null;
              });
        }
      }

      Map<String, String> dbProperties =
          new HashMap<>(properties != null ? properties : new HashMap<>());
      if (!dbProperties.containsKey(HiveNamespaceConfig.DATABASE_LOCATION_URI)) {
        dbProperties.put(
            HiveNamespaceConfig.DATABASE_LOCATION_URI, String.format("%s/%s", root, dbName));
      }

      Database database = new Database();
      database.setName(dbName);
      HiveUtil.setDatabaseProperties(
          database,
          () ->
              ValidationUtil.checkNotNullOrEmptyString(
                  root, "Warehouse location is not set for the Hive Lance namespace backend"),
          dbName,
          dbProperties);

      clientPool.run(
          client -> {
            client.createDatabase(database);
            return null;
          });
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          "Failed to create namespace: " + errorMessage(e),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          "");
    }
  }

  private Map<String, String> doDropDatabase(String db, String mode) {
    try {
      Database database = HiveUtil.getDatabaseOrNull(clientPool, db);
      if (database == null) {
        if (MODE_SKIP.equals(mode)) {
          return new HashMap<>();
        }
        throw new NamespaceNotFoundException(
            String.format("Database %s doesn't exist", db),
            HiveErrorType.HIVE_META_STORE_ERROR.getType(),
            db);
      }

      List<String> tables = doListTables(db);
      if (!tables.isEmpty()) {
        throw new InvalidInputException(
            String.format(
                "Database %s is not empty. Contains %d tables: %s", db, tables.size(), tables),
            HiveErrorType.HIVE_META_STORE_ERROR.getType(),
            db);
      }

      Map<String, String> properties = collectDatabaseProperties(database);
      clientPool.run(
          client -> {
            client.dropDatabase(db, false, true, false);
            return null;
          });

      LOG.info("Successfully dropped database: {}", db);
      return properties;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          "Failed to drop namespace: " + errorMessage(e),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          db);
    }
  }

  private List<String> doListTables(String db) {
    try {
      Database database = HiveUtil.getDatabaseOrNull(clientPool, db);
      if (database == null) {
        throw new NamespaceNotFoundException(
            String.format("Database %s doesn't exist", db),
            HiveErrorType.HIVE_META_STORE_ERROR.getType(),
            db);
      }

      List<String> allTables = clientPool.run(client -> client.getAllTables(db));
      List<String> lanceTables = Lists.newArrayList();
      for (String tableName : allTables) {
        try {
          Optional<Table> table = HiveUtil.getTable(clientPool, db, tableName);
          if (table.isPresent()) {
            Map<String, String> params = table.get().getParameters();
            boolean isLanceTable =
                params != null
                    && HiveUtil.TABLE_TYPE_LANCE.equalsIgnoreCase(
                        params.get(HiveUtil.TABLE_TYPE_PARAM));
            if (isLanceTable) {
              lanceTables.add(tableName);
            }
          }
        } catch (Exception e) {
          LOG.warn("Failed to validate table {}.{}: {}", db, tableName, e.getMessage());
        }
      }
      return lanceTables;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          "Failed to list tables: " + errorMessage(e),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          "");
    }
  }

  private Map<String, String> collectDatabaseProperties(Database database) {
    Map<String, String> properties = new HashMap<>();
    if (database.getDescription() != null) {
      properties.put(HiveNamespaceConfig.DATABASE_DESCRIPTION, database.getDescription());
    }
    if (database.getLocationUri() != null) {
      properties.put(HiveNamespaceConfig.DATABASE_LOCATION_URI, database.getLocationUri());
    }
    if (database.getOwnerName() != null) {
      properties.put(HiveNamespaceConfig.DATABASE_OWNER, database.getOwnerName());
    }
    if (database.getOwnerType() != null) {
      properties.put(HiveNamespaceConfig.DATABASE_OWNER_TYPE, database.getOwnerType().name());
    }
    if (database.getParameters() != null) {
      properties.putAll(database.getParameters());
    }
    return properties;
  }

  private static String errorMessage(Exception e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }
}
