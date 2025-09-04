/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ModelAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";

  private static final String SCHEMA = "SCHEMA";

  private static String role = "role";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    client
        .loadMetalake(METALAKE)
        .createCatalog(CATALOG, Catalog.Type.MODEL, "model", "comment", new HashMap<>())
        .asSchemas()
        .createSchema(SCHEMA, "test", new HashMap<>());
    // try to load the schema as normal user, expect failure
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .loadCatalog(CATALOG)
              .asSchemas()
              .loadSchema(SCHEMA);
        });
    // grant tester privilege
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    gravitinoMetalake.createRole(role, new HashMap<>(), securableObjects);
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role), NORMAL_USER);
    // normal user can load the catalog but not the schema
    Catalog catalogLoadByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals(CATALOG, catalogLoadByNormalUser.name());
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          catalogLoadByNormalUser.asSchemas().loadSchema(SCHEMA);
        });
  }

  @Test
  @Order(1)
  public void testCreateModel() {
    ModelCatalog modelCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    modelCatalog.registerModel(NameIdentifier.of(SCHEMA, "model1"), "", new HashMap<>());
    ModelCatalog normalUserCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          normalUserCatalog.registerModel(NameIdentifier.of(SCHEMA, "model2"), "", new HashMap<>());
        });
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    // test grant create schema privilege
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.CreateModel.allow()));
    normalUserCatalog.registerModel(NameIdentifier.of(SCHEMA, "model2"), "", new HashMap<>());
    normalUserCatalog.registerModel(NameIdentifier.of(SCHEMA, "model3"), "", new HashMap<>());
  }

  @Test
  @Order(2)
  public void testListModel() {
    NameIdentifier[] modelsLoadByNormalUser =
        normalUserClient
            .loadMetalake(METALAKE)
            .loadCatalog(CATALOG)
            .asModelCatalog()
            .listModels(Namespace.of(SCHEMA));
    assertEquals(2, modelsLoadByNormalUser.length);
    List<String> modelNamesLoadByNormalUser =
        Arrays.stream(modelsLoadByNormalUser)
            .map(model -> model.name())
            .collect(Collectors.toList());
    assertLinesMatch(modelNamesLoadByNormalUser, ImmutableList.of("model2", "model3"));
    NameIdentifier[] models =
        client
            .loadMetalake(METALAKE)
            .loadCatalog(CATALOG)
            .asModelCatalog()
            .listModels(Namespace.of(SCHEMA));
    assertEquals(3, models.length);
    List<String> modelNames =
        Arrays.stream(models).map(model -> model.name()).collect(Collectors.toList());
    assertLinesMatch(modelNames, ImmutableList.of("model1", "model2", "model3"));
  }

  @Test
  @Order(3)
  public void testLoadModel() {
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalog = catalogEntityLoadByNormalUser.asModelCatalog();
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalog.getModel(NameIdentifier.of(SCHEMA, "model1"));
        });
    // test grant use model
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.UseModel.allow()));
    Model modelEntity = modelCatalog.getModel(NameIdentifier.of(SCHEMA, "model1"));
    assertEquals("model1", modelEntity.name());
    // reset privilege
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableSet.of(Privileges.UseModel.allow()));
  }

  @Test
  @Order(4)
  public void testAlterModel() {
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalogLoadByNormalUser.alterModel(
              NameIdentifier.of(SCHEMA, "model1"), new ModelChange.RenameModel("model5"));
        });
    ModelCatalog modelCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    modelCatalog.alterModel(
        NameIdentifier.of(SCHEMA, "model1"), new ModelChange.RenameModel("model4"));
    modelCatalog.alterModel(
        NameIdentifier.of(SCHEMA, "model4"), new ModelChange.RenameModel("model1"));
  }

  @Test
  @Order(5)
  public void testDropModel() {
    ModelCatalog modelCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    modelCatalog.registerModel(NameIdentifier.of(SCHEMA, "model5"), "", new HashMap<>());
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalogLoadByNormalUser.deleteModel(NameIdentifier.of(SCHEMA, "model5"));
        });
    modelCatalog.deleteModel(NameIdentifier.of(SCHEMA, "model5"));
  }

  @Test
  @Order(6)
  public void testLinkModel() {
    ModelCatalog modelCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    modelCatalog.linkModelVersion(
        NameIdentifier.of(SCHEMA, "model1"), "uri1", new String[] {"alias1"}, "comment2", null);
    modelCatalog.linkModelVersion(
        NameIdentifier.of(SCHEMA, "model1"), "uri2", new String[] {"alias2"}, "comment2", null);
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "model1" + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalogLoadByNormalUser.linkModelVersion(
              NameIdentifier.of(SCHEMA, "model1"),
              "uri1",
              new String[] {"alias2"},
              "comment2",
              null);
        });
  }

  @Test
  @Order(7)
  public void testListModelVersion() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    ModelCatalog modelCatalog = gravitinoMetalake.loadCatalog(CATALOG).asModelCatalog();
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    int[] versions = modelCatalog.listModelVersions(NameIdentifier.of(SCHEMA, "model1"));
    assertEquals(2, versions.length);
    versions = modelCatalogLoadByNormalUser.listModelVersions(NameIdentifier.of(SCHEMA, "model1"));
    assertEquals(0, versions.length);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "model1"), MetadataObject.Type.MODEL),
        ImmutableSet.of(Privileges.UseModel.allow()));
    versions = modelCatalogLoadByNormalUser.listModelVersions(NameIdentifier.of(SCHEMA, "model1"));
    assertEquals(2, versions.length);
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "model1"), MetadataObject.Type.MODEL),
        ImmutableSet.of(Privileges.UseModel.allow()));
  }

  @Test
  @Order(8)
  public void testLoadModelVersion() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    ModelCatalog modelCatalog = gravitinoMetalake.loadCatalog(CATALOG).asModelCatalog();
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    ModelVersion version = modelCatalog.getModelVersion(NameIdentifier.of(SCHEMA, "model1"), 1);
    assertEquals(1, version.version());
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "model1" + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalogLoadByNormalUser.getModelVersion(NameIdentifier.of(SCHEMA, "model1"), 1);
        });

    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "model1"), MetadataObject.Type.MODEL),
        ImmutableSet.of(Privileges.UseModel.allow()));
    version = modelCatalogLoadByNormalUser.getModelVersion(NameIdentifier.of(SCHEMA, "model1"), 1);
    assertEquals(1, version.version());
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "model1"), MetadataObject.Type.MODEL),
        ImmutableSet.of(Privileges.UseModel.allow()));
  }

  @Test
  @Order(9)
  public void testAlterModelVersion() {
    ModelCatalog modelCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    ModelVersion version =
        modelCatalog.alterModelVersion(
            NameIdentifier.of(SCHEMA, "model1"), 1, ModelVersionChange.setProperty("key", "value"));
    assertEquals("value", version.properties().get("key"));
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "model1" + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalogLoadByNormalUser.alterModelVersion(
              NameIdentifier.of(SCHEMA, "model1"),
              1,
              ModelVersionChange.setProperty("key", "value"));
        });
  }

  @Test
  @Order(10)
  public void testDropModelVersion() {
    ModelCatalog modelCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asModelCatalog();
    Catalog catalogEntityLoadByNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    ModelCatalog modelCatalogLoadByNormalUser = catalogEntityLoadByNormalUser.asModelCatalog();
    assertThrows(
        "Can not access metadata {" + METALAKE + "," + CATALOG + "." + SCHEMA + "model1" + "}.",
        ForbiddenException.class,
        () -> {
          modelCatalogLoadByNormalUser.deleteModelVersion(NameIdentifier.of(SCHEMA, "model1"), 1);
        });
    modelCatalog.deleteModelVersion(NameIdentifier.of(SCHEMA, "model1"), 1);
    int[] versions = modelCatalog.listModelVersions(NameIdentifier.of(SCHEMA, "model1"));
    assertEquals(1, versions.length);
  }
}
