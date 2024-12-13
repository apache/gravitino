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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.authorization.ranger.RangerPathBaseMetadataObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class RangerAuthorizationHDFSPluginIT {

  private static RangerAuthorizationPlugin rangerAuthPlugin;

  @BeforeAll
  public static void setup() {
    RangerITEnv.init(true);
    rangerAuthPlugin = RangerITEnv.rangerAuthHDFSPlugin;
  }

  @AfterAll
  public static void cleanup() {
    RangerITEnv.cleanup();
  }

  @Test
  public void testTranslateMetadataObject() {
    MetadataObject metalake =
        MetadataObjects.parse(String.format("metalake1"), MetadataObject.Type.METALAKE);
    Assertions.assertEquals(
        RangerPathBaseMetadataObject.Type.PATH,
        rangerAuthPlugin.translateMetadataObject(metalake).type());

    MetadataObject catalog =
        MetadataObjects.parse(String.format("catalog1"), MetadataObject.Type.CATALOG);
    Assertions.assertEquals(
        RangerPathBaseMetadataObject.Type.PATH,
        rangerAuthPlugin.translateMetadataObject(catalog).type());

    MetadataObject schema =
        MetadataObjects.parse(String.format("catalog1.schema1"), MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        RangerPathBaseMetadataObject.Type.PATH,
        rangerAuthPlugin.translateMetadataObject(schema).type());

    MetadataObject table =
        MetadataObjects.parse(String.format("catalog1.schema1.tab1"), MetadataObject.Type.TABLE);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> rangerAuthPlugin.translateMetadataObject(table));

    MetadataObject fileset =
        MetadataObjects.parse(
            String.format("catalog1.schema1.fileset1"), MetadataObject.Type.FILESET);
    AuthorizationMetadataObject rangerFileset = rangerAuthPlugin.translateMetadataObject(fileset);
    Assertions.assertEquals(1, rangerFileset.names().size());
    Assertions.assertEquals("/test", rangerFileset.fullName());
    Assertions.assertEquals(RangerPathBaseMetadataObject.Type.PATH, rangerFileset.type());
  }

  @Test
  public void testTranslatePrivilege() {
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
          Assertions.assertEquals(RangerPathBaseMetadataObject.Type.PATH, securableObject.type());
          Assertions.assertEquals("/test", securableObject.fullName());
          Assertions.assertEquals(2, securableObject.privileges().size());
        });
  }

  @Test
  public void testTranslateOwner() {
    MetadataObject metalake =
        MetadataObjects.parse(String.format("metalake1"), MetadataObject.Type.METALAKE);
    List<AuthorizationSecurableObject> metalakeOwner = rangerAuthPlugin.translateOwner(metalake);
    Assertions.assertEquals(0, metalakeOwner.size());

    MetadataObject catalog =
        MetadataObjects.parse(String.format("catalog1"), MetadataObject.Type.CATALOG);
    List<AuthorizationSecurableObject> catalogOwner = rangerAuthPlugin.translateOwner(catalog);
    Assertions.assertEquals(0, catalogOwner.size());

    MetadataObject schema =
        MetadataObjects.parse(String.format("catalog1.schema1"), MetadataObject.Type.SCHEMA);
    List<AuthorizationSecurableObject> schemaOwner = rangerAuthPlugin.translateOwner(schema);
    Assertions.assertEquals(0, schemaOwner.size());

    MetadataObject fileset =
        MetadataObjects.parse(
            String.format("catalog1.schema1.fileset1"), MetadataObject.Type.FILESET);
    List<AuthorizationSecurableObject> filesetOwner = rangerAuthPlugin.translateOwner(fileset);
    Assertions.assertEquals(1, filesetOwner.size());
    Assertions.assertEquals("/test", filesetOwner.get(0).fullName());
    Assertions.assertEquals(RangerPathBaseMetadataObject.Type.PATH, filesetOwner.get(0).type());
    Assertions.assertEquals(3, filesetOwner.get(0).privileges().size());
  }
}
