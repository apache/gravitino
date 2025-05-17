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
package org.apache.gravitino.authorization.ranger.integration.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.PathBasedMetadataObject;
import org.apache.gravitino.authorization.common.PathBasedSecurableObject;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@Tag("gravitino-docker-test")
public class RangerAuthorizationHDFSPluginIT {

  private static RangerAuthorizationPlugin rangerAuthPlugin;
  private static final CatalogDispatcher manager = mock(CatalogDispatcher.class);

  @BeforeAll
  public static void setup() throws Exception {
    RangerITEnv.init(RangerITEnv.currentFunName(), true);
    rangerAuthPlugin = RangerITEnv.rangerAuthHDFSPlugin;
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", manager, true);
    when(manager.listCatalogs(any())).thenReturn(new NameIdentifier[0]);
  }

  @AfterAll
  public static void cleanup() {
    RangerITEnv.cleanup();
  }

  public static void withMockedAuthorizationUtils(Runnable testCode) {
    try (MockedStatic<AuthorizationUtils> authzUtilsMockedStatic =
        Mockito.mockStatic(AuthorizationUtils.class)) {
      authzUtilsMockedStatic
          .when(
              () ->
                  AuthorizationUtils.getMetadataObjectLocation(
                      Mockito.any(NameIdentifier.class), Mockito.any(Entity.EntityType.class)))
          .thenReturn(ImmutableList.of("/test"));
      testCode.run();
    }
  }

  @Test
  public void testTranslateMetadataObject() {
    withMockedAuthorizationUtils(
        () -> {
          MetadataObject metalake =
              MetadataObjects.parse("metalake1", MetadataObject.Type.METALAKE);
          rangerAuthPlugin
              .translateMetadataObject(metalake)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(
                        metalake.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.METALAKE_PATH, pathBasedMetadataObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject catalog = MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG);
          rangerAuthPlugin
              .translateMetadataObject(catalog)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(catalog.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.CATALOG_PATH, pathBasedMetadataObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject schema =
              MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA);
          rangerAuthPlugin
              .translateMetadataObject(schema)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(schema.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.SCHEMA_PATH, pathBasedMetadataObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject table =
              MetadataObjects.parse("catalog1.schema1.tab1", MetadataObject.Type.TABLE);
          rangerAuthPlugin
              .translateMetadataObject(table)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(table.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.TABLE_PATH, securableObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject fileset =
              MetadataObjects.parse("catalog1.schema1.fileset1", MetadataObject.Type.FILESET);
          rangerAuthPlugin
              .translateMetadataObject(fileset)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(fileset.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.FILESET_PATH, securableObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });
        });
  }

  @Test
  public void testTranslatePrivilege() {
    withMockedAuthorizationUtils(
        () -> {
          SecurableObject filesetInMetalake =
              SecurableObjects.parse(
                  "metalake1",
                  MetadataObject.Type.METALAKE,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow()));
          List<AuthorizationSecurableObject> filesetInMetalake1 =
              rangerAuthPlugin.translatePrivilege(filesetInMetalake);
          Assertions.assertEquals(0, filesetInMetalake1.size());

          SecurableObject catalogObject =
              SecurableObjects.parse(
                  "catalog1",
                  MetadataObject.Type.CATALOG,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow(),
                      Privileges.CreateTable.allow(),
                      Privileges.SelectTable.allow(),
                      Privileges.ModifyTable.allow()));
          List<AuthorizationSecurableObject> authzCatalogObjects =
              rangerAuthPlugin.translatePrivilege(catalogObject);
          Assertions.assertEquals(6, authzCatalogObjects.size());
          Assertions.assertEquals(
              4,
              (int)
                  authzCatalogObjects.stream()
                      .filter(
                          authorizationSecurableObject -> {
                            PathBasedMetadataObject pathBasedMetadataObject =
                                ((PathBasedMetadataObject) authorizationSecurableObject);
                            return pathBasedMetadataObject.path().equals("/test/*/*")
                                && pathBasedMetadataObject.recursive();
                          })
                      .count());

          Assertions.assertEquals(
              2,
              (int)
                  authzCatalogObjects.stream()
                      .filter(
                          authorizationSecurableObject -> {
                            PathBasedMetadataObject pathBasedMetadataObject =
                                ((PathBasedMetadataObject) authorizationSecurableObject);
                            return pathBasedMetadataObject.path().equals("/test/*/")
                                && !pathBasedMetadataObject.recursive();
                          })
                      .count());

          SecurableObject schemaObject =
              SecurableObjects.parse(
                  "catalog1.schema1",
                  MetadataObject.Type.SCHEMA,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow(),
                      Privileges.CreateTable.allow(),
                      Privileges.SelectTable.allow(),
                      Privileges.ModifyTable.allow()));
          List<AuthorizationSecurableObject> authzSchemaObjects =
              rangerAuthPlugin.translatePrivilege(schemaObject);
          Assertions.assertEquals(6, authzSchemaObjects.size());
          Assertions.assertEquals(
              4,
              (int)
                  authzSchemaObjects.stream()
                      .filter(
                          authorizationSecurableObject -> {
                            PathBasedMetadataObject pathBasedMetadataObject =
                                ((PathBasedMetadataObject) authorizationSecurableObject);
                            return pathBasedMetadataObject.path().equals("/test/*")
                                && pathBasedMetadataObject.recursive();
                          })
                      .count());

          Assertions.assertEquals(
              2,
              (int)
                  authzSchemaObjects.stream()
                      .filter(
                          authorizationSecurableObject -> {
                            PathBasedMetadataObject pathBasedMetadataObject =
                                ((PathBasedMetadataObject) authorizationSecurableObject);
                            return pathBasedMetadataObject.path().equals("/test")
                                && !pathBasedMetadataObject.recursive();
                          })
                      .count());

          SecurableObject filesetObject =
              SecurableObjects.parse(
                  "catalog1.schema1.fileset1",
                  MetadataObject.Type.FILESET,
                  Lists.newArrayList(
                      Privileges.ReadFileset.allow(), Privileges.WriteFileset.allow()));
          List<AuthorizationSecurableObject> filesetObjects =
              rangerAuthPlugin.translatePrivilege(filesetObject);
          Assertions.assertEquals(2, filesetObjects.size());

          filesetObjects.forEach(
              securableObject -> {
                PathBasedSecurableObject pathBasedSecurableObject =
                    (PathBasedSecurableObject) securableObject;
                Assertions.assertEquals(
                    PathBasedMetadataObject.FILESET_PATH, pathBasedSecurableObject.type());
                Assertions.assertEquals("/test", pathBasedSecurableObject.path());
              });

          SecurableObject tableObject =
              SecurableObjects.parse(
                  "catalog1.schema1.table1",
                  MetadataObject.Type.TABLE,
                  Lists.newArrayList(
                      Privileges.SelectTable.allow(), Privileges.ModifyTable.allow()));
          List<AuthorizationSecurableObject> authzTableObjects =
              rangerAuthPlugin.translatePrivilege(tableObject);
          Assertions.assertEquals(2, authzTableObjects.size());

          authzTableObjects.forEach(
              securableObject -> {
                PathBasedSecurableObject pathBasedSecurableObject =
                    (PathBasedSecurableObject) securableObject;
                Assertions.assertEquals(
                    PathBasedMetadataObject.TABLE_PATH, pathBasedSecurableObject.type());
                Assertions.assertEquals("/test", pathBasedSecurableObject.path());
              });
        });
  }

  @Test
  public void testTranslateOwner() {
    withMockedAuthorizationUtils(
        () -> {
          MetadataObject metalake =
              MetadataObjects.parse("metalake1", MetadataObject.Type.METALAKE);
          List<AuthorizationSecurableObject> metalakeOwner =
              rangerAuthPlugin.translateOwner(metalake);
          Assertions.assertEquals(0, metalakeOwner.size());

          MetadataObject catalog = MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG);
          List<AuthorizationSecurableObject> catalogOwner =
              rangerAuthPlugin.translateOwner(catalog);
          Assertions.assertEquals(1, catalogOwner.size());

          MetadataObject schema =
              MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA);
          List<AuthorizationSecurableObject> schemaOwner = rangerAuthPlugin.translateOwner(schema);
          Assertions.assertEquals(1, schemaOwner.size());

          MetadataObject fileset =
              MetadataObjects.parse("catalog1.schema1.fileset1", MetadataObject.Type.FILESET);
          List<AuthorizationSecurableObject> filesetOwner =
              rangerAuthPlugin.translateOwner(fileset);
          filesetOwner.forEach(
              authorizationSecurableObject -> {
                PathBasedSecurableObject pathBasedSecurableObject =
                    (PathBasedSecurableObject) authorizationSecurableObject;
                Assertions.assertEquals(1, filesetOwner.size());
                Assertions.assertEquals("/test", pathBasedSecurableObject.path());
                Assertions.assertEquals(
                    PathBasedMetadataObject.FILESET_PATH, pathBasedSecurableObject.type());
                Assertions.assertEquals(3, pathBasedSecurableObject.privileges().size());
              });
        });
  }
}
