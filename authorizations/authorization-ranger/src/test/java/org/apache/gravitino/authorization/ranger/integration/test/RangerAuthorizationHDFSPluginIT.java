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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.Entity;
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

  @BeforeAll
  public static void setup() {
    RangerITEnv.init(RangerITEnv.currentFunName(), true);
    rangerAuthPlugin = RangerITEnv.rangerAuthHDFSPlugin;
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
              MetadataObjects.parse(String.format("metalake1"), MetadataObject.Type.METALAKE);
          rangerAuthPlugin
              .translateMetadataObject(metalake)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(
                        metalake.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.PathType.get(MetadataObject.Type.METALAKE),
                        pathBasedMetadataObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject catalog =
              MetadataObjects.parse(String.format("catalog1"), MetadataObject.Type.CATALOG);
          rangerAuthPlugin
              .translateMetadataObject(catalog)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(catalog.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.PathType.get(MetadataObject.Type.CATALOG),
                        pathBasedMetadataObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject schema =
              MetadataObjects.parse(String.format("catalog1.schema1"), MetadataObject.Type.SCHEMA);
          rangerAuthPlugin
              .translateMetadataObject(schema)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(schema.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.PathType.get(MetadataObject.Type.SCHEMA),
                        pathBasedMetadataObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject table =
              MetadataObjects.parse(
                  String.format("catalog1.schema1.tab1"), MetadataObject.Type.TABLE);
          rangerAuthPlugin
              .translateMetadataObject(table)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(table.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.PathType.get(MetadataObject.Type.TABLE),
                        securableObject.type());
                    Assertions.assertEquals("/test", pathBasedMetadataObject.path());
                  });

          MetadataObject fileset =
              MetadataObjects.parse(
                  String.format("catalog1.schema1.fileset1"), MetadataObject.Type.FILESET);
          rangerAuthPlugin
              .translateMetadataObject(fileset)
              .forEach(
                  securableObject -> {
                    PathBasedMetadataObject pathBasedMetadataObject =
                        (PathBasedMetadataObject) securableObject;
                    Assertions.assertEquals(fileset.fullName(), pathBasedMetadataObject.fullName());
                    Assertions.assertEquals(
                        PathBasedMetadataObject.PathType.get(MetadataObject.Type.FILESET),
                        securableObject.type());
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
                  String.format("metalake1"),
                  MetadataObject.Type.METALAKE,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow()));
          List<AuthorizationSecurableObject> filesetInMetalake1 =
              rangerAuthPlugin.translatePrivilege(filesetInMetalake);
          Assertions.assertEquals(0, filesetInMetalake1.size());

          SecurableObject filesetInCatalog =
              SecurableObjects.parse(
                  String.format("catalog1"),
                  MetadataObject.Type.CATALOG,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow()));
          List<AuthorizationSecurableObject> filesetInCatalog1 =
              rangerAuthPlugin.translatePrivilege(filesetInCatalog);
          Assertions.assertEquals(0, filesetInCatalog1.size());

          SecurableObject filesetInSchema =
              SecurableObjects.parse(
                  String.format("catalog1.schema1"),
                  MetadataObject.Type.SCHEMA,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow()));
          List<AuthorizationSecurableObject> filesetInSchema1 =
              rangerAuthPlugin.translatePrivilege(filesetInSchema);
          Assertions.assertEquals(0, filesetInSchema1.size());

          SecurableObject filesetInFileset =
              SecurableObjects.parse(
                  String.format("catalog1.schema1.fileset1"),
                  MetadataObject.Type.FILESET,
                  Lists.newArrayList(
                      Privileges.CreateFileset.allow(),
                      Privileges.ReadFileset.allow(),
                      Privileges.WriteFileset.allow()));
          List<AuthorizationSecurableObject> filesetInFileset1 =
              rangerAuthPlugin.translatePrivilege(filesetInFileset);
          Assertions.assertEquals(2, filesetInFileset1.size());

          filesetInFileset1.forEach(
              securableObject -> {
                PathBasedSecurableObject pathBasedSecurableObject =
                    (PathBasedSecurableObject) securableObject;
                Assertions.assertEquals(
                    PathBasedMetadataObject.PathType.get(MetadataObject.Type.FILESET),
                    pathBasedSecurableObject.type());
                Assertions.assertEquals("/test", pathBasedSecurableObject.path());
                Assertions.assertEquals(2, pathBasedSecurableObject.privileges().size());
              });
        });
  }

  @Test
  public void testTranslateOwner() {
    withMockedAuthorizationUtils(
        () -> {
          MetadataObject metalake =
              MetadataObjects.parse(String.format("metalake1"), MetadataObject.Type.METALAKE);
          List<AuthorizationSecurableObject> metalakeOwner =
              rangerAuthPlugin.translateOwner(metalake);
          Assertions.assertEquals(0, metalakeOwner.size());

          MetadataObject catalog =
              MetadataObjects.parse(String.format("catalog1"), MetadataObject.Type.CATALOG);
          List<AuthorizationSecurableObject> catalogOwner =
              rangerAuthPlugin.translateOwner(catalog);
          Assertions.assertEquals(0, catalogOwner.size());

          MetadataObject schema =
              MetadataObjects.parse(String.format("catalog1.schema1"), MetadataObject.Type.SCHEMA);
          List<AuthorizationSecurableObject> schemaOwner = rangerAuthPlugin.translateOwner(schema);
          Assertions.assertEquals(0, schemaOwner.size());

          MetadataObject fileset =
              MetadataObjects.parse(
                  String.format("catalog1.schema1.fileset1"), MetadataObject.Type.FILESET);
          List<AuthorizationSecurableObject> filesetOwner =
              rangerAuthPlugin.translateOwner(fileset);
          filesetOwner.forEach(
              authorizationSecurableObject -> {
                PathBasedSecurableObject pathBasedSecurableObject =
                    (PathBasedSecurableObject) authorizationSecurableObject;
                Assertions.assertEquals(1, filesetOwner.size());
                Assertions.assertEquals("/test", pathBasedSecurableObject.path());
                Assertions.assertEquals(
                    PathBasedMetadataObject.PathType.get(MetadataObject.Type.FILESET),
                    pathBasedSecurableObject.type());
                Assertions.assertEquals(3, pathBasedSecurableObject.privileges().size());
              });
        });
  }
}
