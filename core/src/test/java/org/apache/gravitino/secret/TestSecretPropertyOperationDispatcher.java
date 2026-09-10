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
package org.apache.gravitino.secret;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ModelOperationDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.catalog.TopicOperationDispatcher;
import org.apache.gravitino.catalog.ViewOperationDispatcher;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SecretPropertyOperationDispatcher#getSecrets} for each entity-type loader,
 * including the metalake / table / topic / view / model branches that were previously untested.
 */
public class TestSecretPropertyOperationDispatcher extends TestOperationDispatcher {

  private static final String SCHEMA = "secret_schema";
  private static final Map<String, String> SECRET_PROPS =
      ImmutableMap.of("k1", "v1", "jdbc-password", "s3cr3t", "visible", "ok");

  private static SecretPropertyOperationDispatcher secretDispatcher;
  private static SchemaOperationDispatcher schemaOperationDispatcher;
  private static TableOperationDispatcher tableOperationDispatcher;
  private static TopicOperationDispatcher topicOperationDispatcher;
  private static ViewOperationDispatcher viewOperationDispatcher;
  private static ModelOperationDispatcher modelOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    Supplier<SchemaDispatcher> schemaDispatcherSupplier = () -> schemaOperationDispatcher;
    viewOperationDispatcher =
        new ViewOperationDispatcher(
            catalogManager, entityStore, idGenerator, schemaDispatcherSupplier, secretManager);
    modelOperationDispatcher =
        new ModelOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    secretDispatcher =
        new SecretPropertyOperationDispatcher(
            catalogManager, entityStore, idGenerator, secretManager);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(Configs.TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(Configs.TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(Configs.TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalSchemaDispatcher", schemaOperationDispatcher, true);

    schemaOperationDispatcher.createSchema(
        NameIdentifier.of(metalake, catalog, SCHEMA), "comment", ImmutableMap.of("k1", "v1"));
  }

  @Test
  public void testGetSecretsForMetalake() throws IOException {
    BaseMetalake stored =
        entityStore.get(
            NameIdentifier.of(metalake), Entity.EntityType.METALAKE, BaseMetalake.class);
    BaseMetalake updated =
        BaseMetalake.builder()
            .withId(stored.id())
            .withName(stored.name())
            .withComment(stored.comment())
            .withProperties(SECRET_PROPS)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();
    entityStore.put(updated, true);

    Map<String, String> secrets =
        secretDispatcher.getSecrets(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);
    Assertions.assertEquals("s3cr3t", secrets.get("jdbc-password"));
    Assertions.assertFalse(secrets.containsKey("visible"));
  }

  @Test
  public void testGetSecretsForTable() {
    NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, SCHEMA, "secret_table");
    Column[] columns = {Column.of("id", Types.IntegerType.get())};
    tableOperationDispatcher.createTable(
        tableIdent, columns, "comment", SECRET_PROPS, new Transform[0]);

    Map<String, String> secrets = secretDispatcher.getSecrets(tableIdent, Entity.EntityType.TABLE);
    Assertions.assertEquals("s3cr3t", secrets.get("jdbc-password"));
    Assertions.assertFalse(secrets.containsKey("visible"));
  }

  @Test
  public void testGetSecretsForTopic() {
    NameIdentifier topicIdent = NameIdentifier.of(metalake, catalog, SCHEMA, "secret_topic");
    topicOperationDispatcher.createTopic(topicIdent, "comment", null, SECRET_PROPS);

    Map<String, String> secrets = secretDispatcher.getSecrets(topicIdent, Entity.EntityType.TOPIC);
    Assertions.assertEquals("s3cr3t", secrets.get("jdbc-password"));
    Assertions.assertFalse(secrets.containsKey("visible"));
  }

  @Test
  public void testGetSecretsForView() {
    NameIdentifier viewIdent = NameIdentifier.of(metalake, catalog, SCHEMA, "secret_view");
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build()
        };
    viewOperationDispatcher.createView(
        viewIdent, "comment", new Column[0], representations, null, null, SECRET_PROPS);

    Map<String, String> secrets = secretDispatcher.getSecrets(viewIdent, Entity.EntityType.VIEW);
    Assertions.assertEquals("s3cr3t", secrets.get("jdbc-password"));
    Assertions.assertFalse(secrets.containsKey("visible"));
  }

  @Test
  public void testGetSecretsForModel() {
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, SCHEMA, "secret_model");
    modelOperationDispatcher.registerModel(modelIdent, "comment", SECRET_PROPS);

    Map<String, String> secrets = secretDispatcher.getSecrets(modelIdent, Entity.EntityType.MODEL);
    Assertions.assertEquals("s3cr3t", secrets.get("jdbc-password"));
    Assertions.assertFalse(secrets.containsKey("visible"));
  }
}
