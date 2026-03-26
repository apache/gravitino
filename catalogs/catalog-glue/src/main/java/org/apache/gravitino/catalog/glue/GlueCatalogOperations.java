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
package org.apache.gravitino.catalog.glue;

import static org.apache.gravitino.catalog.glue.GlueCatalogPropertiesMetadata.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueCatalogPropertiesMetadata.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueCatalogPropertiesMetadata.AWS_SECRET_ACCESS_KEY;
import static org.apache.gravitino.catalog.glue.GlueCatalogPropertiesMetadata.AWS_CATALOG_ID;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Column;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class GlueCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GlueCatalogOperations.class);

  private GlueClient glueClient;
  private String catalogId;

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {

    String region = conf.get(AWS_REGION);
    String accessKeyId = conf.get(AWS_ACCESS_KEY_ID);
    String secretAccessKey = conf.get(AWS_SECRET_ACCESS_KEY);

    if (region == null || region.isBlank()) {
      throw new IllegalArgumentException(
              "Missing required property: " + AWS_REGION);
    }

    software.amazon.awssdk.services.glue.GlueClientBuilder builder =
        GlueClient.builder().region(Region.of(region));

    if (accessKeyId != null && secretAccessKey != null) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(accessKeyId, secretAccessKey)));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }

    this.glueClient = builder.build();
    this.catalogId = conf.get(AWS_CATALOG_ID);
    LOG.info("Initialized Glue catalog client for region {}", region);
  }

  @Override
  public void close() {
    if (glueClient != null) {
      glueClient.close();
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    List<NameIdentifier> schemas = new ArrayList<>();
    String nextToken = null;
    do {
      GetDatabasesRequest.Builder req = GetDatabasesRequest.builder();
      if (nextToken != null) req.nextToken(nextToken);
      if (catalogId != null) req.catalogId(catalogId);
      var response = glueClient.getDatabases(req.build());
      response.databaseList().stream()
          .map(db -> NameIdentifier.of(namespace, db.name()))
          .forEach(schemas::add);
      nextToken = response.nextToken();
    } while (nextToken != null);

    return schemas.toArray(new NameIdentifier[0]);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      var db =
          glueClient
              .getDatabase(GetDatabaseRequest.builder()
                      .name(ident.name())
                      .catalogId(catalogId)  // add this line
                      .build())
              .database();

      return new GlueSchema(
              ident.name(),
              db.description(),
              db.parameters() != null ? db.parameters() : Collections.emptyMap(),
              AuditInfo.builder()
                      .withCreator("glue")
                      .withCreateTime(db.createTime() != null ? db.createTime() : Instant.now())
                      .build());

    } catch (EntityNotFoundException e) {
      throw new NoSuchSchemaException("Schema %s does not exist in Glue", ident.name());
    }
  }

  @Override
  public Table createTable(
          NameIdentifier tableIdent,
          org.apache.gravitino.rel.Column[] gravitinoColumns,
          String comment,
          Map<String, String> properties,
          Transform[] partitioning,
          Distribution distribution,
          SortOrder[] sortOrders,
          Index[] indexes)
          throws NoSuchSchemaException, TableAlreadyExistsException {

    String dbName = tableIdent.namespace().level(tableIdent.namespace().length() - 1);

    // Separate partition column names from regular columns
    java.util.Set<String> partitionColNames =
            Arrays.stream(partitioning)
                    .filter(t -> "identity".equals(t.name()))
                    .map(t -> t.references()[0].fieldName()[0])
                    .collect(Collectors.toSet());

    List<Column> sdColumns = new ArrayList<>();
    List<Column> partitionKeys = new ArrayList<>();

    for (org.apache.gravitino.rel.Column col : gravitinoColumns) {
      Column glueCol =
              Column.builder()
                      .name(col.name())
                      .type(GlueTypeConverter.toGlue(col.dataType()))
                      .comment(col.comment())
                      .build();

      if (partitionColNames.contains(col.name())) {
        partitionKeys.add(glueCol);
      } else {
        sdColumns.add(glueCol);
      }
    }

    StorageDescriptor sd =
            StorageDescriptor.builder()
                    .columns(sdColumns)
                    .build();

    TableInput.Builder tableInput =
            TableInput.builder()
                    .name(tableIdent.name())
                    .description(comment)
                    .storageDescriptor(sd)
                    .partitionKeys(partitionKeys);

    if (properties != null && !properties.isEmpty()) {
      tableInput.parameters(properties);
    }

    try {
      glueClient.createTable(
              CreateTableRequest.builder()
                      .databaseName(dbName)
                      .tableInput(tableInput.build())
                      .build());

      LOG.info("Created Glue table {}.{}", dbName, tableIdent.name());
      return loadTable(tableIdent);

    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(
              "Table %s already exists in Glue database %s", tableIdent.name(), dbName);
    }
  }

  @Override
  public Schema createSchema(
          NameIdentifier ident, String comment, Map<String, String> properties)
          throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      DatabaseInput.Builder dbInput =
              DatabaseInput.builder()
                      .name(ident.name())
                      .description(comment);

      if (properties != null && !properties.isEmpty()) {
        dbInput.parameters(properties);
      }

      glueClient.createDatabase(
              CreateDatabaseRequest.builder().databaseInput(dbInput.build()).build());

      LOG.info("Created Glue database {}", ident.name());

      return new GlueSchema(
              ident.name(),
              comment,
              properties != null ? properties : Collections.emptyMap(),
              AuditInfo.builder()
                      .withCreator("glue")
                      .withCreateTime(Instant.now())
                      .build());

    } catch (AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
              "Schema %s already exists in Glue", ident.name());
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
          throws NoSuchSchemaException {
    // Load current state
    var db =
            glueClient
                    .getDatabase(GetDatabaseRequest.builder().name(ident.name()).build())
                    .database();

    String description = db.description();
    Map<String, String> parameters =
            db.parameters() != null ? new HashMap<>(db.parameters()) : new HashMap<>();

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty set = (SchemaChange.SetProperty) change;
        parameters.put(set.getProperty(), set.getValue());
      } else if (change instanceof SchemaChange.RemoveProperty remove) {
          parameters.remove(remove.getProperty());
      } else {
        throw new UnsupportedOperationException(
                "Glue catalog does not support schema change: "
                        + change.getClass().getSimpleName());
      }
    }

    DatabaseInput updatedInput =
            DatabaseInput.builder()
                    .name(ident.name())
                    .description(description)
                    .parameters(parameters)
                    .build();

    glueClient.updateDatabase(
            UpdateDatabaseRequest.builder()
                    .name(ident.name())
                    .databaseInput(updatedInput)
                    .build());

    LOG.info("Altered Glue database {}", ident.name());
    return loadSchema(ident);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade)
          throws NonEmptySchemaException {
    try {
      if (cascade) {
        // Delete all tables first
        String[] parentLevels = ident.namespace().levels();
        String[] schemaLevels = Arrays.copyOf(parentLevels, parentLevels.length + 1);
        schemaLevels[parentLevels.length] = ident.name();
        NameIdentifier[] tables = listTables(Namespace.of(schemaLevels));
        for (NameIdentifier table : tables) {
          dropTable(table);
        }
      }

      glueClient.deleteDatabase(
              DeleteDatabaseRequest.builder().name(ident.name()).build());

      LOG.info("Dropped Glue database {}", ident.name());
      return true;

    } catch (EntityNotFoundException e) {
      return false;
    }
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String dbName = namespace.level(namespace.length() - 1);
    List<NameIdentifier> tables = new ArrayList<>();
    String nextToken = null;
    do {
      GetTablesRequest.Builder req = GetTablesRequest.builder().databaseName(dbName);
      if (nextToken != null) req.nextToken(nextToken);
      if (catalogId != null) req.catalogId(catalogId);
      var response = glueClient.getTables(req.build());
      response.tableList().stream()
          .map(t -> NameIdentifier.of(namespace, t.name()))
          .forEach(tables::add);
      nextToken = response.nextToken();
    } while (nextToken != null);

    return tables.toArray(new NameIdentifier[0]);
  }

  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    String dbName = tableIdent.namespace().level(tableIdent.namespace().length() - 1);
    try {
      var glueTable =
          glueClient
              .getTable(GetTableRequest.builder()
                              .databaseName(dbName)
                              .name(tableIdent.name())
                              .catalogId(catalogId).build())
              .table();

      return GlueTable.fromGlueTable(glueTable, tableIdent);

    } catch (EntityNotFoundException e) {
      throw new NoSuchTableException("Table %s does not exist in Glue", tableIdent.name());
    }
  }

  @Override
  public Table alterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("Glue catalog does not support altering tables");
  }

  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    String dbName = tableIdent.namespace().level(tableIdent.namespace().length() - 1);
    try {
      glueClient.deleteTable(
              DeleteTableRequest.builder()
                      .databaseName(dbName)
                      .name(tableIdent.name())
                      .build());

      LOG.info("Dropped Glue table {}.{}", dbName, tableIdent.name());
      return true;

    } catch (EntityNotFoundException e) {
      return false;
    }
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    try {
      glueClient.getDatabases(GetDatabasesRequest.builder().maxResults(1).build());
    } catch (Exception e) {
      throw new ConnectionFailedException(e, "Failed to connect to AWS Glue: %s", e.getMessage());
    }
  }
}