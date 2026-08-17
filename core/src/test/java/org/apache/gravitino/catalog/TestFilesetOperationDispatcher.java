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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.StringIdentifier.ID_KEY;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestFilesetOperationDispatcher extends TestOperationDispatcher {
  static FilesetOperationDispatcher filesetOperationDispatcher;
  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
  }

  public static FilesetOperationDispatcher getFilesetOperationDispatcher() {
    return filesetOperationDispatcher;
  }

  public static SchemaOperationDispatcher getSchemaOperationDispatcher() {
    return schemaOperationDispatcher;
  }

  @Test
  public void testCreateAndListFilesets() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema81");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset1");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "test", props);
    Assertions.assertEquals("fileset1", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("test", fileset1.storageLocation());

    NameIdentifier[] idents = filesetOperationDispatcher.listFilesets(filesetNs);
    Assertions.assertEquals(1, idents.length);
    Assertions.assertEquals(filesetIdent1, idents[0]);

    Map<String, String> illegalProps = ImmutableMap.of("k2", "v2");
    testPropertyException(
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps),
        "Properties or property prefixes are required and must be set");

    Map<String, String> illegalProps2 = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps2),
        "Properties or property prefixes are reserved and cannot be set",
        "gravitino.identifier");
  }

  @Test
  public void testCreateAndLoadFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema91");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "location", "schema91");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset11");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, null, props);
    Assertions.assertEquals("fileset11", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertNull(fileset1.storageLocation());

    Fileset loadedFileset1 = filesetOperationDispatcher.loadFileset(filesetIdent1);
    Assertions.assertEquals(fileset1.name(), loadedFileset1.name());
    Assertions.assertEquals(fileset1.comment(), loadedFileset1.comment());
    testProperties(props, loadedFileset1.properties());
    Assertions.assertEquals(fileset1.type(), loadedFileset1.type());
    Assertions.assertEquals(fileset1.storageLocation(), loadedFileset1.storageLocation());
  }

  @Test
  public void testCreateAndAlterFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema101");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset21");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "fileset21", props);
    Assertions.assertEquals("fileset21", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("fileset21", fileset1.storageLocation());

    FilesetChange[] changes =
        new FilesetChange[] {
          FilesetChange.setProperty("k3", "v3"), FilesetChange.removeProperty("k1")
        };

    Fileset alteredFileset = filesetOperationDispatcher.alterFileset(filesetIdent1, changes);
    Assertions.assertEquals(fileset1.name(), alteredFileset.name());
    Assertions.assertEquals(fileset1.comment(), alteredFileset.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredFileset.properties());

    FilesetChange[] changes2 = new FilesetChange[] {FilesetChange.updateComment("new comment")};

    Fileset alteredFileset2 = filesetOperationDispatcher.alterFileset(filesetIdent1, changes2);
    Assertions.assertEquals(fileset1.name(), alteredFileset2.name());
    Assertions.assertEquals("new comment", alteredFileset2.comment());

    FilesetChange[] changes3 = new FilesetChange[] {FilesetChange.updateComment(null)};

    Fileset alteredFileset3 = filesetOperationDispatcher.alterFileset(filesetIdent1, changes3);
    Assertions.assertEquals(fileset1.name(), alteredFileset3.name());
    Assertions.assertNull(alteredFileset3.comment());

    // Test immutable fileset properties
    FilesetChange[] illegalChange = new FilesetChange[] {FilesetChange.setProperty(ID_KEY, "test")};
    testPropertyException(
        () -> filesetOperationDispatcher.alterFileset(filesetIdent1, illegalChange),
        "Property gravitino.identifier is immutable or reserved, cannot be set");
  }

  @Test
  public void testCreateAndDropFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema111");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset31");
    Fileset fileset1 =
        filesetOperationDispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "fileset31", props);
    Assertions.assertEquals("fileset31", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("fileset31", fileset1.storageLocation());

    boolean dropped = filesetOperationDispatcher.dropFileset(filesetIdent1);
    Assertions.assertTrue(dropped);
    Assertions.assertFalse(filesetOperationDispatcher.dropFileset(filesetIdent1));
  }

  @Test
  public void testCreateAndGetFileLocation() {
    String tmpDir = "/tmp/test_get_file_location_" + UUID.randomUUID();
    try {
      Namespace filesetNs = Namespace.of(metalake, catalog, "schema1024");
      Map<String, String> props = ImmutableMap.of("k1", "v1", "location", "schema1024");
      schemaOperationDispatcher.createSchema(
          NameIdentifier.of(filesetNs.levels()), "comment", props);

      NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset1024");
      Fileset fileset1 =
          filesetOperationDispatcher.createFileset(
              filesetIdent1, "comment", Fileset.Type.MANAGED, tmpDir, props);
      Assertions.assertEquals("fileset1024", fileset1.name());
      Assertions.assertEquals("comment", fileset1.comment());
      testProperties(props, fileset1.properties());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertNotNull(fileset1.storageLocation());

      // test sub path starts with "/"
      String subPath1 = "/test/test.parquet";
      String fileLocation1 = filesetOperationDispatcher.getFileLocation(filesetIdent1, subPath1);
      Assertions.assertEquals(
          String.format("%s%s", fileset1.storageLocation(), subPath1), fileLocation1);

      // test sub path not starts with "/"
      String subPath2 = "test/test.parquet";
      String fileLocation2 = filesetOperationDispatcher.getFileLocation(filesetIdent1, subPath2);
      Assertions.assertEquals(
          String.format("%s/%s", fileset1.storageLocation(), subPath2), fileLocation2);

      // test sub path is null
      String subPath3 = null;
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> filesetOperationDispatcher.getFileLocation(filesetIdent1, subPath3));

      // test sub path is blank but not null
      String subPath4 = "";
      String fileLocation3 = filesetOperationDispatcher.getFileLocation(filesetIdent1, subPath4);
      Assertions.assertEquals(fileset1.storageLocation(), fileLocation3);

      // test mount a single file
      String filesetName2 = "test_get_file_location_2";
      String filesetLocation2 = "/tmp/test_get_file_location_" + UUID.randomUUID();
      NameIdentifier filesetIdent2 = NameIdentifier.of(filesetNs, filesetName2);
      filesetOperationDispatcher.createFileset(
          filesetIdent2, "comment", Fileset.Type.MANAGED, filesetLocation2, props);
      File localFile2 = new File(filesetLocation2);
      try {
        // replace dir to file
        if (localFile2.exists()) {
          localFile2.delete();
        }
        localFile2.createNewFile();

        String subPath = "/year=2024/month=07/day=22/test.parquet";
        Map<String, String> contextMap = Maps.newHashMap();
        contextMap.put(
            FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
            FilesetDataOperation.RENAME.name());
        CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
        CallerContext.CallerContextHolder.set(callerContext);

        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> filesetOperationDispatcher.getFileLocation(filesetIdent2, subPath));
      } catch (IOException e) {
        // ignore
      } finally {
        CallerContext.CallerContextHolder.remove();
        if (localFile2.exists()) {
          localFile2.delete();
        }
      }

      // test rename with an empty subPath
      String filesetName3 = "test_get_file_location_3";
      String filesetLocation3 = "/tmp/test_get_file_location_" + UUID.randomUUID();
      NameIdentifier filesetIdent3 = NameIdentifier.of(filesetNs, filesetName3);
      filesetOperationDispatcher.createFileset(
          filesetIdent3, "comment", Fileset.Type.MANAGED, filesetLocation3, props);
      File localFile3 = new File(filesetLocation3);
      try {
        // replace dir to file
        if (localFile3.exists()) {
          localFile3.delete();
        }
        localFile3.createNewFile();

        String subPath = "";
        Map<String, String> contextMap = Maps.newHashMap();
        contextMap.put(
            FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
            FilesetDataOperation.RENAME.name());
        CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
        CallerContext.CallerContextHolder.set(callerContext);

        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> filesetOperationDispatcher.getFileLocation(filesetIdent2, subPath));
      } catch (IOException e) {
        // ignore
      } finally {
        CallerContext.CallerContextHolder.remove();
        if (localFile3.exists()) {
          localFile3.delete();
        }
      }

      // test storage location end with "/"
      String filesetName4 = "test_get_file_location_4";
      String filesetLocation4 = "/tmp/test_get_file_location_" + UUID.randomUUID() + "/";
      NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, filesetName4);
      filesetOperationDispatcher.createFileset(
          filesetIdent, "comment", Fileset.Type.MANAGED, filesetLocation4, props);
      String subPath = "/test/test.parquet";
      String fileLocation = filesetOperationDispatcher.getFileLocation(filesetIdent, subPath);
      Assertions.assertEquals(
          String.format("%s%s", filesetLocation4, subPath.substring(1)), fileLocation);
    } finally {
      File path = new File(tmpDir);
      if (path.exists()) {
        path.delete();
      }
    }
  }

  @Test
  public void testCreateWithSecrets() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      AtomicLong nextId = new AtomicLong(9000L);
      IdGenerator ids = nextId::getAndIncrement;
      FilesetOperationDispatcher filesets =
          new FilesetOperationDispatcher(catalogManager, entityStore, ids, secrets);
      new SchemaOperationDispatcher(catalogManager, entityStore, ids, secrets)
          .createSchema(
              NameIdentifier.of(metalake, catalog, "schema_secret_fileset"),
              "comment",
              ImmutableMap.of("k1", "v1"));

      NameIdentifier ident =
          NameIdentifier.of(metalake, catalog, "schema_secret_fileset", "fileset_secret_1");
      Map<String, SecretBinding> bindings = Map.of("k2", new SecretBinding("memory", "s3cr3t"));
      Map<String, String> locations = Map.of(Fileset.LOCATION_NAME_UNKNOWN, "loc");
      Map<String, String> props = ImmutableMap.of("k1", "v1");
      long entityId = nextId.get();
      Fileset fileset =
          filesets.createMultipleLocationFileset(
              ident, "comment", Fileset.Type.MANAGED, locations, props, bindings, Map.of());
      Assertions.assertFalse(fileset.properties().containsKey("k2"));

      Fileset stored =
          catalogManager
              .loadCatalogAndWrap(NameIdentifier.of(metalake, catalog))
              .doWithFilesetOps(ops -> ops.loadFileset(ident));
      Assertions.assertTrue(
          SecretPropertyUtils.isSecretProperty("k2", stored.properties().get("k2")));

      SecretUrn urn =
          SecretUrn.buildWriteThrough(
              "memory",
              Map.of(
                  SecretConstants.ATTR_ENTITY_TYPE, "fileset",
                  SecretConstants.ATTR_ENTITY_ID, String.valueOf(entityId),
                  SecretConstants.ATTR_PROPERTY_KEY, "k2"));
      Assertions.assertEquals("s3cr3t", secrets.readSecret(urn));
      Assertions.assertThrows(
          FilesetAlreadyExistsException.class,
          () ->
              filesets.createMultipleLocationFileset(
                  ident, "comment", Fileset.Type.MANAGED, locations, props, bindings, Map.of()));
      Assertions.assertTrue(filesets.dropFileset(ident));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
    }
  }

  @Test
  public void testAlterFilesetRemovePropertyDeletesWriteThroughSecret() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      AtomicLong nextId = new AtomicLong(9200L);
      IdGenerator ids = nextId::getAndIncrement;
      FilesetOperationDispatcher filesets =
          new FilesetOperationDispatcher(catalogManager, entityStore, ids, secrets);
      String schemaName = "schema_secret_alter_" + UUID.randomUUID();
      new SchemaOperationDispatcher(catalogManager, entityStore, ids, secrets)
          .createSchema(
              NameIdentifier.of(metalake, catalog, schemaName),
              "comment",
              ImmutableMap.of("k1", "v1"));

      NameIdentifier ident = NameIdentifier.of(metalake, catalog, schemaName, "fileset_alter_1");
      Map<String, SecretBinding> bindings = Map.of("k2", new SecretBinding("memory", "s3cr3t"));
      Map<String, String> locations = Map.of(Fileset.LOCATION_NAME_UNKNOWN, "loc");
      Map<String, String> props = ImmutableMap.of("k1", "v1");
      long entityId = nextId.get();
      filesets.createMultipleLocationFileset(
          ident, "comment", Fileset.Type.MANAGED, locations, props, bindings, Map.of());

      SecretUrn urn =
          SecretUrn.buildWriteThrough(
              "memory",
              Map.of(
                  SecretConstants.ATTR_ENTITY_TYPE, "fileset",
                  SecretConstants.ATTR_ENTITY_ID, String.valueOf(entityId),
                  SecretConstants.ATTR_PROPERTY_KEY, "k2"));
      Assertions.assertEquals("s3cr3t", secrets.readSecret(urn));

      filesets.alterFileset(ident, FilesetChange.removeProperty("k2"));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
    }
  }

  @Test
  public void testDropMetalakeForceCleansFilesetSecrets() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      Object previous = FieldUtils.readField(GravitinoEnv.getInstance(), "secretManager", true);
      FieldUtils.writeField(GravitinoEnv.getInstance(), "secretManager", secrets, true);
      try {
        AtomicLong nextId = new AtomicLong(9300L);
        IdGenerator ids = nextId::getAndIncrement;
        String ml = "ml_force_secret_" + UUID.randomUUID().toString().replace("-", "");
        String cat = "cat_force_secret";
        String schemaName = "schema_force_secret";

        entityStore.put(
            org.apache.gravitino.meta.BaseMetalake.builder()
                .withId(ids.nextId())
                .withName(ml)
                .withAuditInfo(
                    org.apache.gravitino.meta.AuditInfo.builder()
                        .withCreator("test")
                        .withCreateTime(java.time.Instant.now())
                        .build())
                .withVersion(org.apache.gravitino.meta.SchemaVersion.V_0_1)
                .build(),
            true);

        CatalogManager cm = new CatalogManager(new Config(false) {}, entityStore, ids, secrets);
        cm.createCatalog(
            NameIdentifier.of(ml, cat),
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            "test",
            "comment",
            ImmutableMap.of(
                org.apache.gravitino.TestCatalog.PROPERTY_KEY1,
                "value1",
                org.apache.gravitino.TestCatalog.PROPERTY_KEY2,
                "value2",
                org.apache.gravitino.TestCatalog.PROPERTY_KEY5_PREFIX + "1",
                "value3"));

        SchemaOperationDispatcher schemas =
            new SchemaOperationDispatcher(cm, entityStore, ids, secrets);
        FilesetOperationDispatcher filesets =
            new FilesetOperationDispatcher(cm, entityStore, ids, secrets);
        schemas.createSchema(
            NameIdentifier.of(ml, cat, schemaName), "comment", ImmutableMap.of("k1", "v1"));

        NameIdentifier filesetIdent = NameIdentifier.of(ml, cat, schemaName, "fs_force_1");
        long entityId = nextId.get();
        filesets.createMultipleLocationFileset(
            filesetIdent,
            "comment",
            Fileset.Type.MANAGED,
            Map.of(Fileset.LOCATION_NAME_UNKNOWN, "loc"),
            ImmutableMap.of("k1", "v1"),
            Map.of("k2", new SecretBinding("memory", "force-secret")),
            Map.of());

        SecretUrn urn =
            SecretUrn.buildWriteThrough(
                "memory",
                Map.of(
                    SecretConstants.ATTR_ENTITY_TYPE, "fileset",
                    SecretConstants.ATTR_ENTITY_ID, String.valueOf(entityId),
                    SecretConstants.ATTR_PROPERTY_KEY, "k2"));
        Assertions.assertEquals("force-secret", secrets.readSecret(urn));

        MetalakeManager metalakeManager = new MetalakeManager(entityStore, ids);
        Assertions.assertTrue(metalakeManager.dropMetalake(NameIdentifier.of(ml), true));
        Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
      } finally {
        FieldUtils.writeField(GravitinoEnv.getInstance(), "secretManager", previous, true);
      }
    }
  }

  private static SecretManager memorySecretManager() {
    Config c = new Config(false) {};
    Properties p = new Properties();
    p.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    p.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    c.loadFromProperties(p);
    return new SecretManager(c);
  }
}
