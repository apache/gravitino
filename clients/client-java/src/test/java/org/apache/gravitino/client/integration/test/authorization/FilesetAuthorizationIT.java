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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FilesetAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private String role = "role";
  private String defaultBaseLocation;
  protected CloseableHttpClient httpClient;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    client
        .loadMetalake(METALAKE)
        .createCatalog(
            CATALOG, Catalog.Type.FILESET, "hadoop", "comment", ImmutableMap.of("k1", "k2"))
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

    httpClient = HttpClients.createDefault();
  }

  @AfterAll
  public void closeHttpClient() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  @Test
  @Order(1)
  public void testCreateFileset() {
    // admin user can create fileset
    FilesetCatalog filesetCatalog =
        client.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    String filename1 = GravitinoITUtils.genRandomName("FilesetAuthorizationIT_fileset1");
    filesetCatalog.createFileset(
        // NameIdentifier.of(SCHEMA, "fileset1"),
        NameIdentifier.of(SCHEMA, "fileset1"),
        "comment",
        Fileset.Type.MANAGED,
        storageLocation(filename1),
        new HashMap<>());
    // normal use cannot create fileset
    FilesetCatalog filesetCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          filesetCatalogNormalUser.createFileset(
              //              NameIdentifier.of(SCHEMA, "fileset2"),
              NameIdentifier.of(SCHEMA, "fileset2"),
              "comment",
              Fileset.Type.MANAGED,
              storageLocation(GravitinoITUtils.genRandomName("FilesetAuthorizationIT_fileset2")),
              new HashMap<>());
        });

    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> filesetCatalogNormalUser.listFilesets(Namespace.of(SCHEMA)));

    // grant privileges
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.CreateFileset.allow()));
    // normal user can now create fileset
    String filename2 = GravitinoITUtils.genRandomName("FilesetAuthorizationIT_fileset2");
    filesetCatalogNormalUser.createFileset(
        NameIdentifier.of(SCHEMA, "fileset2"),
        "comment",
        Fileset.Type.MANAGED,
        storageLocation(filename2),
        new HashMap<>());
    String filename3 = GravitinoITUtils.genRandomName("FilesetAuthorizationIT_fileset3");
    filesetCatalogNormalUser.createFileset(
        NameIdentifier.of(SCHEMA, "fileset3"),
        "comment",
        Fileset.Type.MANAGED,
        storageLocation(filename3),
        new HashMap<>());
  }

  @Test
  @Order(2)
  public void testListFileset() {
    FilesetCatalog tableCatalog =
        client.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    NameIdentifier[] tablesList = tableCatalog.listFilesets(Namespace.of(SCHEMA));
    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA, "fileset1"),
          NameIdentifier.of(SCHEMA, "fileset2"),
          NameIdentifier.of(SCHEMA, "fileset3"),
        },
        tablesList);
    // normal user can only see filesets which they have privilege for
    FilesetCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    NameIdentifier[] filesetsListNormalUser =
        tableCatalogNormalUser.listFilesets(Namespace.of(SCHEMA));
    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA, "fileset2"), NameIdentifier.of(SCHEMA, "fileset3")
        },
        filesetsListNormalUser);
  }

  @Test
  @Order(3)
  public void testLoadFileset() {
    FilesetCatalog filesetCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    // normal user can load fileset2 and fileset3, but not fileset1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "fileset1"),
        ForbiddenException.class,
        () -> {
          filesetCatalogNormalUser.loadFileset(NameIdentifier.of(SCHEMA, "fileset1"));
        });
    Fileset fileset2 = filesetCatalogNormalUser.loadFileset(NameIdentifier.of(SCHEMA, "fileset2"));
    assertEquals("fileset2", fileset2.name());
    Fileset fileset3 = filesetCatalogNormalUser.loadFileset(NameIdentifier.of(SCHEMA, "fileset3"));
    assertEquals("fileset3", fileset3.name());

    // grant normal user privilege to use fileset1
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        ImmutableList.of(Privileges.ReadFileset.allow()));
    Fileset fileset1 = filesetCatalogNormalUser.loadFileset(NameIdentifier.of(SCHEMA, "fileset1"));
    assertEquals("fileset1", fileset1.name());
  }

  @Test
  @Order(4)
  public void testAlterFileset() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    FilesetCatalog filesetCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();

    // normal user cannot alter fileset1 (no privilege)
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "fileset1"),
        ForbiddenException.class,
        () -> {
          filesetCatalogNormalUser.alterFileset(
              NameIdentifier.of(SCHEMA, "fileset1"), FilesetChange.setProperty("key", "value"));
        });
    // grant normal user write privilege on fileset1
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        ImmutableList.of(Privileges.WriteFileset.allow()));
    filesetCatalogNormalUser.alterFileset(
        NameIdentifier.of(SCHEMA, "fileset1"), FilesetChange.setProperty("key", "value"));
  }

  @Test
  @Order(5)
  public void testGetFileLocation() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    FilesetCatalog filesetCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    // reset privilege
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        ImmutableSet.of(Privileges.WriteFileset.allow(), Privileges.ReadFileset.allow()));

    // normal user cannot alter fileset1 (no privilege)
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "fileset1"),
        ForbiddenException.class,
        () -> {
          filesetCatalogNormalUser.getFileLocation(NameIdentifier.of(SCHEMA, "fileset1"), "/test");
        });
    // grant normal user owner privilege on fileset4
    gravitinoMetalake.setOwner(
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        NORMAL_USER,
        Owner.Type.USER);
    filesetCatalogNormalUser.getFileLocation(NameIdentifier.of(SCHEMA, "fileset1"), "/test");
    // reset
    gravitinoMetalake.setOwner(
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        USER,
        Owner.Type.USER);
  }

  @Test
  @Order(6)
  public void testListFiles() throws IOException {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    FilesetCatalog filesetCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    // reset privilege
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        ImmutableSet.of(Privileges.WriteFileset.allow(), Privileges.ReadFileset.allow()));

    // normal user cannot alter fileset1 (no privilege)
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "fileset1"),
        ForbiddenException.class,
        () -> getFileInfos("fileset1", "/test", "", NORMAL_USER));
    // grant normal user owner privilege on fileset4
    gravitinoMetalake.setOwner(
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        NORMAL_USER,
        Owner.Type.USER);
    filesetCatalogNormalUser.getFileLocation(NameIdentifier.of(SCHEMA, "fileset1"), "/test");
    getFileInfos("fileset1", "/test", "", NORMAL_USER);
    // reset
    gravitinoMetalake.setOwner(
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        USER,
        Owner.Type.USER);
  }

  @Test
  @Order(7)
  public void testDropFileset() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    FilesetCatalog filesetCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(
            ImmutableList.of(CATALOG, SCHEMA, "fileset1"), MetadataObject.Type.FILESET),
        USER,
        Owner.Type.USER);
    // normal user cannot drop fileset1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "fileset1"),
        ForbiddenException.class,
        () -> {
          filesetCatalogNormalUser.dropFileset(NameIdentifier.of(SCHEMA, "fileset1"));
        });
    // normal user can drop fileset2 and fileset3 (they created them)
    filesetCatalogNormalUser.dropFileset(NameIdentifier.of(SCHEMA, "fileset2"));
    filesetCatalogNormalUser.dropFileset(NameIdentifier.of(SCHEMA, "fileset3"));

    // owner can drop fileset1
    FilesetCatalog filesetCatalog =
        client.loadMetalake(METALAKE).loadCatalog(CATALOG).asFilesetCatalog();
    filesetCatalog.dropFileset(NameIdentifier.of(SCHEMA, "fileset1"));
    // check filesets are dropped
    NameIdentifier[] filesetsList = filesetCatalog.listFilesets(Namespace.of(SCHEMA));
    assertArrayEquals(new NameIdentifier[] {}, filesetsList);
    NameIdentifier[] filesetsListNormalUser =
        filesetCatalogNormalUser.listFilesets(Namespace.of(SCHEMA));
    assertArrayEquals(new NameIdentifier[] {}, filesetsListNormalUser);
  }

  private String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      defaultBaseLocation =
          String.format(
              "hdfs://%s:%d/user/hive/%s",
              containerSuite.getHiveContainer().getContainerIpAddress(),
              HiveContainer.HDFS_DEFAULTFS_PORT,
              SCHEMA.toLowerCase());
    }
    return defaultBaseLocation;
  }

  private String storageLocation(String filesetName) {
    return defaultBaseLocation() + "/" + filesetName;
  }

  private void getFileInfos(String fileset, String subPath, String locationName, String user)
      throws IOException {
    String targetPath =
        "/api/metalakes/"
            + METALAKE
            + "/catalogs/"
            + CATALOG
            + "/schemas/"
            + SCHEMA
            + "/filesets/"
            + fileset
            + "/files";

    URIBuilder uriBuilder;
    try {
      uriBuilder = new URIBuilder(serverUri + targetPath);
    } catch (URISyntaxException e) {
      throw new IOException("Error constructing URI: " + serverUri + targetPath, e);
    }

    if (!StringUtils.isBlank(subPath)) {
      uriBuilder.addParameter("subPath", subPath);
    }
    if (!StringUtils.isBlank(locationName)) {
      uriBuilder.addParameter("locationName", locationName);
    }

    HttpGet httpGet;
    try {
      httpGet = new HttpGet(uriBuilder.build());
      String authorization =
          (AuthConstants.AUTHORIZATION_BASIC_HEADER
              + new String(
                  Base64.getEncoder().encode(user.getBytes(StandardCharsets.UTF_8)),
                  StandardCharsets.UTF_8));
      httpGet.addHeader(HttpHeaders.AUTHORIZATION, authorization);
    } catch (URISyntaxException e) {
      throw new IOException("Failed to build URI with query parameters: " + uriBuilder, e);
    }
    try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
      if (httpResponse.getStatusLine().getStatusCode() == 403) {
        throw new ForbiddenException("Can not get files");
      }
    }
  }
}
