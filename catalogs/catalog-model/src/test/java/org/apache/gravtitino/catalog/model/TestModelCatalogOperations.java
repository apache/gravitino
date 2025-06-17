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
package org.apache.gravtitino.catalog.model;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.model.ModelCatalogOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestModelCatalogOperations {

  private static final String STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String METALAKE_NAME = "metalake_for_model_meta_test";

  private static final String CATALOG_NAME = "catalog_for_model_meta_test";

  private static EntityStore store;

  private static IdGenerator idGenerator;

  private static ModelCatalogOperations ops;

  @BeforeAll
  public static void setUp() throws IOException {
    Config config = Mockito.mock(Config.class);
    when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(STORE_PATH);

    // The following properties are used to create the JDBC connection; they are just for test, in
    // the real world,
    // they will be set automatically by the configuration file if you set ENTITY_RELATIONAL_STORE
    // as EMBEDDED_ENTITY_RELATIONAL_STORE.
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);

    when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    idGenerator = new RandomIdGenerator();

    // Create the metalake and catalog
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE_NAME)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(auditInfo)
            .withName(METALAKE_NAME)
            .build();
    store.put(metalake, false);

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG_NAME)
            .withNamespace(Namespace.of(METALAKE_NAME))
            .withProvider("model")
            .withType(Catalog.Type.MODEL)
            .withAuditInfo(auditInfo)
            .build();
    store.put(catalog, false);

    ops = new ModelCatalogOperations(store);
    ops.initialize(
        Collections.emptyMap(),
        Mockito.mock(CatalogInfo.class),
        Mockito.mock(HasPropertyMetadata.class));
  }

  @AfterAll
  public static void tearDown() throws IOException {
    ops.close();
    store.close();
    FileUtils.deleteDirectory(new File(STORE_PATH));
  }

  @Test
  public void testSchemaOperations() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.createSchema(schemaIdent, "schema comment", properties);
    Schema loadedSchema = ops.loadSchema(schemaIdent);

    Assertions.assertEquals(schemaName, loadedSchema.name());
    Assertions.assertEquals("schema comment", loadedSchema.comment());
    Assertions.assertEquals(properties, loadedSchema.properties());

    // Test create schema with the same name
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> ops.createSchema(schemaIdent, "schema comment", properties));

    // Test create schema in a non-existent catalog
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () ->
            ops.createSchema(
                NameIdentifierUtil.ofSchema(METALAKE_NAME, "non-existent-catalog", schemaName),
                "schema comment",
                properties));

    // Test load a non-existent schema
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            ops.loadSchema(
                NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, "non-existent-schema")));

    // Test load a non-existent schema in a non-existent catalog
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            ops.loadSchema(
                NameIdentifierUtil.ofSchema(
                    METALAKE_NAME, "non-existent-catalog", "non-existent-schema")));

    // Create another schema
    String schemaName2 = randomSchemaName();
    NameIdentifier schemaIdent2 =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName2);
    StringIdentifier stringId2 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties2 = StringIdentifier.newPropertiesWithId(stringId2, null);

    ops.createSchema(schemaIdent2, "schema comment 2", properties2);

    // Test list schemas
    NameIdentifier[] idents = ops.listSchemas(Namespace.of(METALAKE_NAME, CATALOG_NAME));

    Set<NameIdentifier> resultSet = Arrays.stream(idents).collect(Collectors.toSet());
    Assertions.assertTrue(resultSet.contains(schemaIdent));
    Assertions.assertTrue(resultSet.contains(schemaIdent2));

    // Test list schemas in a non-existent catalog
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () -> ops.listSchemas(Namespace.of(METALAKE_NAME, "non-existent-catalog")));

    // Test drop schema
    Assertions.assertTrue(ops.dropSchema(schemaIdent, false));
    Assertions.assertFalse(ops.dropSchema(schemaIdent, false));
    Assertions.assertTrue(ops.dropSchema(schemaIdent2, false));
    Assertions.assertFalse(ops.dropSchema(schemaIdent2, false));

    // Test drop non-existent schema
    Assertions.assertFalse(
        ops.dropSchema(
            NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, "non-existent-schema"),
            false));

    // Test drop schema in a non-existent catalog
    Assertions.assertFalse(
        ops.dropSchema(
            NameIdentifierUtil.ofSchema(METALAKE_NAME, "non-existent-catalog", schemaName2),
            false));
  }

  @Test
  public void testRegisterAndGetModel() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    Model registeredModel = ops.registerModel(modelIdent, "model comment", properties);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals("model comment", registeredModel.comment());
    Assertions.assertEquals(properties, registeredModel.properties());
    Assertions.assertEquals(0, registeredModel.latestVersion());

    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(modelName, loadedModel.name());
    Assertions.assertEquals("model comment", loadedModel.comment());
    Assertions.assertEquals(properties, loadedModel.properties());
    Assertions.assertEquals(0, loadedModel.latestVersion());

    // Test register model with the same name
    Assertions.assertThrows(
        ModelAlreadyExistsException.class,
        () -> ops.registerModel(modelIdent, "model comment", properties));

    // Test register model in a non-existent schema
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            ops.registerModel(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName),
                "model comment",
                properties));

    // Test get a non-existent model
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.getModel(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model")));

    // Test get a model in a non-existent schema
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.getModel(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName)));
  }

  @Test
  public void testRegisterAndListModel() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName1 = "model1";
    NameIdentifier modelIdent1 =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName1);
    StringIdentifier stringId1 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties1 = StringIdentifier.newPropertiesWithId(stringId1, null);

    ops.registerModel(modelIdent1, "model1 comment", properties1);

    NameIdentifier[] modelIdents =
        ops.listModels(Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName));
    Assertions.assertEquals(1, modelIdents.length);
    Assertions.assertEquals(modelIdent1, modelIdents[0]);

    String modelName2 = "model2";
    NameIdentifier modelIdent2 =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName2);
    StringIdentifier stringId2 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties2 = StringIdentifier.newPropertiesWithId(stringId2, null);

    ops.registerModel(modelIdent2, "model2 comment", properties2);

    NameIdentifier[] modelIdents2 =
        ops.listModels(Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName));
    Assertions.assertEquals(2, modelIdents2.length);

    Set<NameIdentifier> resultSet = Arrays.stream(modelIdents2).collect(Collectors.toSet());
    Assertions.assertTrue(resultSet.contains(modelIdent1));
    Assertions.assertTrue(resultSet.contains(modelIdent2));

    // Test list models in a non-existent schema
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> ops.listModels(Namespace.of(METALAKE_NAME, CATALOG_NAME, "non-existent-schema")));
  }

  @Test
  public void testRegisterAndDeleteModel() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, "model1 comment", properties);

    Assertions.assertTrue(ops.deleteModel(modelIdent));
    Assertions.assertFalse(ops.deleteModel(modelIdent));

    // Test get a deleted model
    Assertions.assertThrows(NoSuchModelException.class, () -> ops.getModel(modelIdent));

    // Test list models after deletion
    Assertions.assertEquals(
        0, ops.listModels(Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaName)).length);

    // Test delete non-existent model
    Assertions.assertFalse(
        ops.deleteModel(
            NameIdentifierUtil.ofModel(
                METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model")));

    // Test delete model in a non-existent schema
    Assertions.assertFalse(
        ops.deleteModel(
            NameIdentifierUtil.ofModel(
                METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName)));
  }

  @Test
  public void testLinkAndGetModelVersion() {
    // Create schema and model
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, "model1 comment", properties);

    // Link a model version to the registered model
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    String[] aliases = new String[] {"alias1", "alias2"};
    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);

    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertEquals("version1 comment", loadedVersion.comment());
    Assertions.assertEquals("model_version_path", loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // Test get a model version using alias
    ModelVersion loadedVersionByAlias = ops.getModelVersion(modelIdent, "alias1");
    Assertions.assertEquals(0, loadedVersionByAlias.version());

    ModelVersion loadedVersionByAlias2 = ops.getModelVersion(modelIdent, "alias2");
    Assertions.assertEquals(0, loadedVersionByAlias2.version());

    // Test link model version to a non-existent model
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.linkModelVersion(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model"),
                "model_version_path",
                aliases,
                "version1 comment",
                versionProperties));

    // Test link model version to a non-existent schema
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.linkModelVersion(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName),
                "model_version_path",
                aliases,
                "version1 comment",
                versionProperties));

    // Test link model version with existent aliases
    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            ops.linkModelVersion(
                modelIdent,
                "model_version_path",
                new String[] {"alias1"},
                "version1 comment",
                versionProperties));

    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            ops.linkModelVersion(
                modelIdent,
                "model_version_path",
                new String[] {"alias2"},
                "version1 comment",
                versionProperties));

    // Test get a model version from non-existent model
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () ->
            ops.getModelVersion(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model"),
                0));

    // Test get a non-existent model version
    Assertions.assertThrows(
        NoSuchModelVersionException.class, () -> ops.getModelVersion(modelIdent, 1));

    // Test get a non-existent model version using alias
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> ops.getModelVersion(modelIdent, "non-existent-alias"));

    // Test create a model version with null alias, comment and properties
    StringIdentifier versionId2 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties2 = StringIdentifier.newPropertiesWithId(versionId2, null);

    ops.linkModelVersion(modelIdent, "model_version_path2", null, null, versionProperties2);

    // Test get a model version with null alias, comment and properties
    ModelVersion loadedVersion2 = ops.getModelVersion(modelIdent, 1);
    Assertions.assertEquals(1, loadedVersion2.version());
    Assertions.assertNull(loadedVersion2.comment());
    Assertions.assertEquals(versionProperties2, loadedVersion2.properties());
    Assertions.assertEquals(0, loadedVersion2.aliases().length);

    // Test get a model version with alias
    Assertions.assertThrows(
        NoSuchModelVersionException.class, () -> ops.getModelVersion(modelIdent, "alias3"));
  }

  @Test
  public void testLinkAndListModelVersions() {
    // Create schema and model
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, "model1 comment", properties);

    // Create a model version
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    String[] aliases = new String[] {"alias1", "alias2"};
    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);

    // List linked model versions
    int[] versions = ops.listModelVersions(modelIdent);
    Assertions.assertEquals(1, versions.length);
    Assertions.assertEquals(0, versions[0]);

    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, versions[0]);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertEquals("version1 comment", loadedVersion.comment());
    Assertions.assertEquals("model_version_path", loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // Create another model version
    StringIdentifier versionId2 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties2 = StringIdentifier.newPropertiesWithId(versionId2, null);

    String[] aliases2 = new String[] {"alias3", "alias4"};
    ops.linkModelVersion(
        modelIdent, "model_version_path2", aliases2, "version2 comment", versionProperties2);

    // List linked model versions
    int[] versions2 = ops.listModelVersions(modelIdent);
    Assertions.assertEquals(2, versions2.length);

    Set<Integer> resultSet = Arrays.stream(versions2).boxed().collect(Collectors.toSet());
    Assertions.assertTrue(resultSet.contains(0));
    Assertions.assertTrue(resultSet.contains(1));

    // Test list model versions in a non-existent model
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.listModelVersions(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model")));

    // Test list model versions in a non-existent schema
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.listModelVersions(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName)));
  }

  @Test
  public void testLinkAndListModelVersionInfos() {
    // Create schema and model
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, "model1 comment", properties);

    // Create a model version
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    String[] aliases = new String[] {"alias1", "alias2"};
    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);

    // List linked model versions info
    ModelVersion[] versions = ops.listModelVersionInfos(modelIdent);
    Assertions.assertEquals(1, versions.length);
    Assertions.assertEquals(0, versions[0].version());
    Assertions.assertEquals("version1 comment", versions[0].comment());
    Assertions.assertEquals("model_version_path", versions[0].uri());
    Assertions.assertEquals(versionProperties, versions[0].properties());

    // Create another model version
    StringIdentifier versionId2 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties2 = StringIdentifier.newPropertiesWithId(versionId2, null);

    String[] aliases2 = new String[] {"alias3", "alias4"};
    ops.linkModelVersion(
        modelIdent, "model_version_path2", aliases2, "version2 comment", versionProperties2);

    // List linked model versions
    ModelVersion[] versions2 = ops.listModelVersionInfos(modelIdent);
    Assertions.assertEquals(2, versions2.length);
    Assertions.assertEquals(0, versions2[0].version());
    Assertions.assertEquals("version1 comment", versions2[0].comment());
    Assertions.assertEquals("model_version_path", versions2[0].uri());
    Assertions.assertEquals(versionProperties, versions2[0].properties());
    Assertions.assertEquals(1, versions2[1].version());
    Assertions.assertEquals("version2 comment", versions2[1].comment());
    Assertions.assertEquals("model_version_path2", versions2[1].uri());
    Assertions.assertEquals(versionProperties2, versions2[1].properties());

    // Test list model versions in a non-existent model
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.listModelVersionInfos(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model")));

    // Test list model versions in a non-existent schema
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            ops.listModelVersionInfos(
                NameIdentifierUtil.ofModel(
                    METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName)));
  }

  @Test
  public void testDeleteModelVersion() {
    // Create schema and model
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, "model1 comment", properties);

    // Create a model version
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    String[] aliases = new String[] {"alias1", "alias2"};
    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);

    // Delete the model version
    Assertions.assertTrue(ops.deleteModelVersion(modelIdent, 0));
    Assertions.assertFalse(ops.deleteModelVersion(modelIdent, 0));

    // Test get a deleted model version
    Assertions.assertThrows(
        NoSuchModelVersionException.class, () -> ops.getModelVersion(modelIdent, 0));

    // Test delete model version in a non-existent model
    Assertions.assertFalse(
        ops.deleteModelVersion(
            NameIdentifierUtil.ofModel(
                METALAKE_NAME, CATALOG_NAME, schemaName, "non-existent-model"),
            0));

    // Test delete model version in a non-existent schema
    Assertions.assertFalse(
        ops.deleteModelVersion(
            NameIdentifierUtil.ofModel(
                METALAKE_NAME, CATALOG_NAME, "non-existent-schema", modelName),
            0));

    // Test delete model version using alias
    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);

    Assertions.assertTrue(ops.deleteModelVersion(modelIdent, "alias1"));
    Assertions.assertFalse(ops.deleteModelVersion(modelIdent, "alias1"));
    Assertions.assertFalse(ops.deleteModelVersion(modelIdent, "alias2"));

    // Test list model versions after deletion
    Assertions.assertEquals(0, ops.listModelVersions(modelIdent).length);

    // Test get the latest version after deletion
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(2, loadedModel.latestVersion());

    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);
    int[] versions = ops.listModelVersions(modelIdent);
    Assertions.assertEquals(1, versions.length);
    Assertions.assertEquals(2, versions[0]);
  }

  @Test
  public void testDeleteModelWithVersions() {
    // Create schema and model
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, "model1 comment", properties);

    // Create a model version
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    String[] aliases = new String[] {"alias1", "alias2"};
    ops.linkModelVersion(
        modelIdent, "model_version_path", aliases, "version1 comment", versionProperties);

    // Delete the model
    Assertions.assertTrue(ops.deleteModel(modelIdent));
    Assertions.assertFalse(ops.deleteModel(modelIdent));

    // Test get a deleted model
    Assertions.assertThrows(NoSuchModelException.class, () -> ops.getModel(modelIdent));

    // Test list model versions after deletion
    Assertions.assertThrows(NoSuchModelException.class, () -> ops.listModelVersions(modelIdent));
  }

  @Test
  public void testRenameModel() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model";
    String newModelName = "new_model";
    String comment = "comment";

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    Model registeredModel = ops.registerModel(modelIdent, comment, properties);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals(comment, registeredModel.comment());
    Assertions.assertEquals(properties, registeredModel.properties());

    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(modelName, loadedModel.name());
    Assertions.assertEquals(comment, loadedModel.comment());
    Assertions.assertEquals(properties, loadedModel.properties());

    ModelChange change = ModelChange.rename(newModelName);
    Model alteredModel = ops.alterModel(modelIdent, change);

    Assertions.assertEquals(newModelName, alteredModel.name());
    Assertions.assertEquals(comment, alteredModel.comment());
    Assertions.assertEquals(properties, alteredModel.properties());
  }

  @Test
  void testAddModelProperty() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model";
    String comment = "comment";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(stringId, ImmutableMap.of("key1", "value1"));
    Map<String, String> newProperties =
        ImmutableMap.<String, String>builder().putAll(properties).put("key2", "value2").build();

    // validate registered model
    Model registeredModel = ops.registerModel(modelIdent, comment, properties);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals(comment, registeredModel.comment());
    Assertions.assertEquals(properties, registeredModel.properties());

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(modelName, loadedModel.name());
    Assertions.assertEquals(comment, loadedModel.comment());
    Assertions.assertEquals(properties, loadedModel.properties());

    ModelChange change = ModelChange.setProperty("key2", "value2");
    Model alteredModel = ops.alterModel(modelIdent, change);

    // validate altered model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(comment, alteredModel.comment());
    Assertions.assertEquals(newProperties, alteredModel.properties());
  }

  @Test
  void testUpdateModelProperty() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model";
    String comment = "comment";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(stringId, ImmutableMap.of("key1", "value1"));
    Map<String, String> newProperties =
        StringIdentifier.newPropertiesWithId(stringId, ImmutableMap.of("key1", "value2"));

    // validate registered model
    Model registeredModel = ops.registerModel(modelIdent, comment, properties);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals(comment, registeredModel.comment());
    Assertions.assertEquals(properties, registeredModel.properties());

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(modelName, loadedModel.name());
    Assertions.assertEquals(comment, loadedModel.comment());
    Assertions.assertEquals(properties, loadedModel.properties());

    ModelChange change = ModelChange.setProperty("key1", "value2");
    Model alteredModel = ops.alterModel(modelIdent, change);

    // validate altered model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(comment, alteredModel.comment());
    Assertions.assertEquals(newProperties, alteredModel.properties());
  }

  @Test
  void testRemoveModelProperty() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model";
    String comment = "comment";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(stringId, ImmutableMap.of("key1", "value1"));
    Map<String, String> newProperties =
        StringIdentifier.newPropertiesWithId(stringId, ImmutableMap.of());

    // validate registered model
    Model registeredModel = ops.registerModel(modelIdent, comment, properties);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals(comment, registeredModel.comment());
    Assertions.assertEquals(properties, registeredModel.properties());

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(modelName, loadedModel.name());
    Assertions.assertEquals(comment, loadedModel.comment());
    Assertions.assertEquals(properties, loadedModel.properties());

    ModelChange change = ModelChange.removeProperty("key1");
    Model alteredModel = ops.alterModel(modelIdent, change);

    // validate altered model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(comment, alteredModel.comment());
    Assertions.assertEquals(newProperties, alteredModel.properties());
  }

  @Test
  void testUpdateModelComment() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model";
    String comment = "comment";
    String newComment = "new comment";

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(stringId, ImmutableMap.of("key1", "value1"));

    // validate registered model
    Model registeredModel = ops.registerModel(modelIdent, comment, properties);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals(comment, registeredModel.comment());
    Assertions.assertEquals(properties, registeredModel.properties());

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(modelName, loadedModel.name());
    Assertions.assertEquals(comment, loadedModel.comment());
    Assertions.assertEquals(properties, loadedModel.properties());

    ModelChange change = ModelChange.updateComment(newComment);
    Model alteredModel = ops.alterModel(modelIdent, change);

    // validate altered model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(newComment, alteredModel.comment());
    Assertions.assertEquals(properties, alteredModel.properties());
  }

  @Test
  void testUpdateVersionCommentViaVersion() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionNewComment = "new version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // update comment via version and validate
    ModelVersionChange updateCommentChange = ModelVersionChange.updateComment(versionNewComment);
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, 0, updateCommentChange);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionNewComment, updatedModelVersion.comment());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());
  }

  @Test
  void testUpdateVersionCommentViaAlias() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionNewComment = "new version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String versionAlias = versionAliases[0];

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties = StringIdentifier.newPropertiesWithId(versionId, null);

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    Model loadedModel = ops.getModel(modelIdent);

    // validate loaded model
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version via alias
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, versionAlias);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // update comment via alias and validate
    ModelVersionChange updateCommentChange = ModelVersionChange.updateComment(versionNewComment);
    ModelVersion updatedModelVersion =
        ops.alterModelVersion(modelIdent, versionAlias, updateCommentChange);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(versionNewComment, updatedModelVersion.comment());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());
  }

  @Test
  void testSetAndUpdateModelVersionProperty() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));
    Map<String, String> tmpMap = Maps.newHashMap(versionProperties);
    tmpMap.put("key3", "value3");
    tmpMap.put("key1", "new value");
    Map<String, String> newProperties = ImmutableMap.copyOf(tmpMap);

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // set property via version and validate
    ModelVersionChange updatePropertyChange = ModelVersionChange.setProperty("key1", "new value");
    ModelVersionChange addPropertyChange = ModelVersionChange.setProperty("key3", "value3");

    ModelVersion updatedModelVersion =
        ops.alterModelVersion(modelIdent, 0, updatePropertyChange, addPropertyChange);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(newProperties, updatedModelVersion.properties());
  }

  @Test
  void testSetAndUpdateModelVersionPropertyByAlias() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));
    Map<String, String> tmpMap = Maps.newHashMap(versionProperties);
    tmpMap.put("key3", "value3");
    tmpMap.put("key1", "new value");
    Map<String, String> newProperties = ImmutableMap.copyOf(tmpMap);

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, versionAliases[0]);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // set property via version and validate
    ModelVersionChange updatePropertyChange = ModelVersionChange.setProperty("key1", "new value");
    ModelVersionChange addPropertyChange = ModelVersionChange.setProperty("key3", "value3");

    ModelVersion updatedModelVersion =
        ops.alterModelVersion(
            modelIdent, versionAliases[0], updatePropertyChange, addPropertyChange);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(newProperties, updatedModelVersion.properties());
  }

  @Test
  void testRemoveModelVersionProperty() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));
    Map<String, String> newVersionProperties =
        StringIdentifier.newPropertiesWithId(versionId, ImmutableMap.of("key1", "value1"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // set property via version and validate
    ModelVersionChange removeProperty = ModelVersionChange.removeProperty("key2");

    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, 0, removeProperty);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(newVersionProperties, updatedModelVersion.properties());
  }

  @Test
  void testRemoveModelVersionPropertyByAlias() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));
    Map<String, String> newVersionProperties =
        StringIdentifier.newPropertiesWithId(versionId, ImmutableMap.of("key1", "value1"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, versionAliases[0]);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // set property via version and validate
    ModelVersionChange removeProperty = ModelVersionChange.removeProperty("key2");

    ModelVersion updatedModelVersion =
        ops.alterModelVersion(modelIdent, versionAliases[0], removeProperty);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(newVersionProperties, updatedModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionUri() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String newVersionUri = "s3://bucket/path/to/new/version";

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version uri
    ModelVersionChange updateUriChange = ModelVersionChange.updateUri(newVersionUri);
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, 0, updateUriChange);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(newVersionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionUriByAlias() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String newVersionUri = "s3://bucket/path/to/new/version";

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, versionAliases[0]);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version uri
    ModelVersionChange updateUriChange = ModelVersionChange.updateUri(newVersionUri);
    ModelVersion updatedModelVersion =
        ops.alterModelVersion(modelIdent, versionAliases[0], updateUriChange);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(newVersionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(versionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());
  }

  @Test
  void testUpdateModelAlias() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String[] newVersionAliases = new String[] {"new_alias1", "new_alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version aliases
    ModelVersionChange change = ModelVersionChange.updateAliases(newVersionAliases, versionAliases);
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(newVersionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());

    // Reload the version
    ModelVersion reloadVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, reloadVersion.version());
    Assertions.assertArrayEquals(newVersionAliases, reloadVersion.aliases());
    Assertions.assertEquals(versionUri, reloadVersion.uri());
    Assertions.assertEquals(versionComment, reloadVersion.comment());
    Assertions.assertEquals(versionProperties, reloadVersion.properties());
  }

  @Test
  void testUpdateModelAliasByAlias() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String[] newVersionAliases = new String[] {"new_alias1", "new_alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, "alias1");
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version aliases
    ModelVersionChange change = ModelVersionChange.updateAliases(newVersionAliases, versionAliases);
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, "alias1", change);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(newVersionAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());

    // Reload the version
    ModelVersion reloadVersion = ops.getModelVersion(modelIdent, "new_alias2");
    Assertions.assertEquals(0, reloadVersion.version());
    Assertions.assertArrayEquals(newVersionAliases, reloadVersion.aliases());
    Assertions.assertEquals(versionUri, reloadVersion.uri());
    Assertions.assertEquals(versionComment, reloadVersion.comment());
    Assertions.assertEquals(versionProperties, reloadVersion.properties());
  }

  @Test
  void testUpdateModelVersionWithPartialAliasChanges() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model2";
    String modelComment = "model2 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String[] newVersionAliases = new String[] {"new_alias1", "new_alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version aliases
    ModelVersionChange change =
        ModelVersionChange.updateAliases(newVersionAliases, new String[] {"alias1"});
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, updatedModelVersion.aliases());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());

    // Reload the version
    ModelVersion reloadVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, reloadVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, reloadVersion.aliases());
    Assertions.assertEquals(versionUri, reloadVersion.uri());
    Assertions.assertEquals(versionComment, reloadVersion.comment());
    Assertions.assertEquals(versionProperties, reloadVersion.properties());
  }

  @Test
  void testUpdateModelVersionByAliasWithPartialAliasChanges() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model3";
    String modelComment = "model3 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias1", "alias2"};
    String[] newVersionAliases = new String[] {"new_alias1", "new_alias2"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, "alias1");
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version aliases
    ModelVersionChange change =
        ModelVersionChange.updateAliases(newVersionAliases, new String[] {"alias1"});
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, "alias1", change);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, updatedModelVersion.aliases());
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());

    // Reload the version
    ModelVersion reloadVersion = ops.getModelVersion(modelIdent, "new_alias2");
    Assertions.assertEquals(0, reloadVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, reloadVersion.aliases());
    Assertions.assertEquals(versionUri, reloadVersion.uri());
    Assertions.assertEquals(versionComment, reloadVersion.comment());
    Assertions.assertEquals(versionProperties, reloadVersion.properties());
  }

  @Test
  void testUpdateModelVersionAliasesOverlapAddAndRemove() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias2", "alias3"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version aliases
    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"alias1", "alias2"}, new String[] {"alias2", "alias3"});
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(updatedModelVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());

    // Reload the version
    ModelVersion reloadVersion = ops.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, reloadVersion.version());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(reloadVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(versionUri, reloadVersion.uri());
    Assertions.assertEquals(versionComment, reloadVersion.comment());
    Assertions.assertEquals(versionProperties, reloadVersion.properties());
  }

  @Test
  void testUpdateModelVersionAliasesByAliasOverlapAddAndRemove() {
    String schemaName = randomSchemaName();
    createSchema(schemaName);

    String modelName = "model1";
    String modelComment = "model1 comment";

    String versionComment = "version1 comment";
    String versionUri = "model_version_path";
    String[] versionAliases = new String[] {"alias2", "alias3"};

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, schemaName, modelName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.registerModel(modelIdent, modelComment, properties);
    StringIdentifier versionId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> versionProperties =
        StringIdentifier.newPropertiesWithId(
            versionId, ImmutableMap.of("key1", "value1", "key2", "value2"));

    ops.linkModelVersion(modelIdent, versionUri, versionAliases, versionComment, versionProperties);

    // validate loaded model
    Model loadedModel = ops.getModel(modelIdent);
    Assertions.assertEquals(1, loadedModel.latestVersion());

    // validate loaded version
    ModelVersion loadedVersion = ops.getModelVersion(modelIdent, "alias2");
    Assertions.assertEquals(0, loadedVersion.version());
    Assertions.assertArrayEquals(versionAliases, loadedVersion.aliases());
    Assertions.assertEquals(versionComment, loadedVersion.comment());
    Assertions.assertEquals(versionUri, loadedVersion.uri());
    Assertions.assertEquals(versionProperties, loadedVersion.properties());

    // validate update version aliases
    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"alias1", "alias2"}, new String[] {"alias2", "alias3"});
    ModelVersion updatedModelVersion = ops.alterModelVersion(modelIdent, "alias3", change);

    Assertions.assertEquals(0, updatedModelVersion.version());
    Assertions.assertEquals(versionUri, updatedModelVersion.uri());
    Assertions.assertEquals(versionComment, updatedModelVersion.comment());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(updatedModelVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(versionProperties, updatedModelVersion.properties());

    // Reload the version
    ModelVersion reloadVersion = ops.getModelVersion(modelIdent, "alias2");
    Assertions.assertEquals(0, reloadVersion.version());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(reloadVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(versionUri, reloadVersion.uri());
    Assertions.assertEquals(versionComment, reloadVersion.comment());
    Assertions.assertEquals(versionProperties, reloadVersion.properties());
  }

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }

  private void createSchema(String schemaName) {
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.createSchema(schemaIdent, "schema comment", properties);
  }
}
