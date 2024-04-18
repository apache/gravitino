/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class TestFilesetStandardizedDispatcher extends TestFilesetOperationDispatcher {
  private static FilesetStandardizedDispatcher filesetStandardizedDispatcher;
  private static SchemaStandardizedDispatcher schemaStandardizedDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestFilesetOperationDispatcher.initialize();
    filesetStandardizedDispatcher = new FilesetStandardizedDispatcher(filesetOperationDispatcher);
    schemaStandardizedDispatcher = new SchemaStandardizedDispatcher(schemaOperationDispatcher);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema112");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaStandardizedDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    // test case-insensitive in creation
    NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, "filesetNAME");
    Fileset createdFileset =
        filesetStandardizedDispatcher.createFileset(
            filesetIdent, "comment", Fileset.Type.MANAGED, "fileset41", props);
    Assertions.assertEquals(filesetIdent.name().toLowerCase(), createdFileset.name());

    // test case-insensitive in loading
    Fileset loadedFileset = filesetStandardizedDispatcher.loadFileset(filesetIdent);
    Assertions.assertEquals(filesetIdent.name().toLowerCase(), loadedFileset.name());

    // test case-insensitive in listing
    NameIdentifier[] filesets = filesetStandardizedDispatcher.listFilesets(filesetNs);
    Arrays.stream(filesets).forEach(s -> Assertions.assertEquals(s.name().toLowerCase(), s.name()));

    // test case-insensitive in altering
    Fileset alteredFileset =
        filesetStandardizedDispatcher.alterFileset(
            NameIdentifier.of(filesetNs, filesetIdent.name().toLowerCase()),
            FilesetChange.setProperty("k2", "v2"));
    Assertions.assertEquals(filesetIdent.name().toLowerCase(), alteredFileset.name());

    Exception exception =
        Assertions.assertThrows(
            FilesetAlreadyExistsException.class,
            () ->
                filesetStandardizedDispatcher.alterFileset(
                    NameIdentifier.of(filesetNs, filesetIdent.name().toUpperCase()),
                    FilesetChange.rename(filesetIdent.name().toUpperCase())));
    Assertions.assertEquals(
        "Fileset metalake.catalog.schema112.filesetname already exists", exception.getMessage());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        filesetStandardizedDispatcher.dropFileset(
            NameIdentifier.of(filesetNs, filesetIdent.name().toUpperCase())));
    Assertions.assertFalse(filesetStandardizedDispatcher.filesetExists(filesetIdent));
  }

}
