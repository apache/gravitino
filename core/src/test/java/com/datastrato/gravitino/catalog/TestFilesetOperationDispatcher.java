/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.connector.BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestFilesetOperationDispatcher extends TestOperationDispatcher {
  static FilesetOperationDispatcher filesetOperationDispatcher;
  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator);
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
        "Properties are required and must be set");

    Map<String, String> illegalProps2 = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () ->
            filesetOperationDispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps2),
        "Properties are reserved and cannot be set",
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

    // Test immutable fileset properties
    FilesetChange[] illegalChange =
        new FilesetChange[] {FilesetChange.setProperty(GRAVITINO_MANAGED_ENTITY, "test")};
    testPropertyException(
        () -> filesetOperationDispatcher.alterFileset(filesetIdent1, illegalChange),
        "Property gravitino.managed.entity is immutable or reserved, cannot be set");
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
  }
}
