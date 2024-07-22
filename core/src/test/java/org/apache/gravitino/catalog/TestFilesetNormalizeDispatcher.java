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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestFilesetNormalizeDispatcher extends TestOperationDispatcher {
  private static FilesetNormalizeDispatcher filesetNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestFilesetOperationDispatcher.initialize();
    filesetNormalizeDispatcher =
        new FilesetNormalizeDispatcher(
            TestFilesetOperationDispatcher.filesetOperationDispatcher, catalogManager);
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(
            TestFilesetOperationDispatcher.schemaOperationDispatcher, catalogManager);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema112");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    // test case-insensitive in creation
    NameIdentifier filesetIdent = NameIdentifier.of(filesetNs, "filesetNAME");
    Fileset createdFileset =
        filesetNormalizeDispatcher.createFileset(
            filesetIdent, "comment", Fileset.Type.MANAGED, "fileset41", props);
    Assertions.assertEquals(filesetIdent.name().toLowerCase(), createdFileset.name());

    // test case-insensitive in loading
    Fileset loadedFileset = filesetNormalizeDispatcher.loadFileset(filesetIdent);
    Assertions.assertEquals(filesetIdent.name().toLowerCase(), loadedFileset.name());

    // test case-insensitive in listing
    NameIdentifier[] filesets = filesetNormalizeDispatcher.listFilesets(filesetNs);
    Arrays.stream(filesets).forEach(s -> Assertions.assertEquals(s.name().toLowerCase(), s.name()));

    // test case-insensitive in altering
    Fileset alteredFileset =
        filesetNormalizeDispatcher.alterFileset(
            NameIdentifier.of(filesetNs, filesetIdent.name().toLowerCase()),
            FilesetChange.setProperty("k2", "v2"));
    Assertions.assertEquals(filesetIdent.name().toLowerCase(), alteredFileset.name());

    Exception exception =
        Assertions.assertThrows(
            FilesetAlreadyExistsException.class,
            () ->
                filesetNormalizeDispatcher.alterFileset(
                    NameIdentifier.of(filesetNs, filesetIdent.name().toUpperCase()),
                    FilesetChange.rename(filesetIdent.name().toUpperCase())));
    Assertions.assertEquals(
        "Fileset metalake.catalog.schema112.filesetname already exists", exception.getMessage());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        filesetNormalizeDispatcher.dropFileset(
            NameIdentifier.of(filesetNs, filesetIdent.name().toUpperCase())));
    Assertions.assertFalse(filesetNormalizeDispatcher.filesetExists(filesetIdent));
  }

  @Test
  public void testNameSpec() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "testNameSpec");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent =
        NameIdentifier.of(filesetNs, MetadataObjects.METADATA_OBJECT_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                filesetNormalizeDispatcher.createFileset(
                    filesetIdent, "comment", Fileset.Type.MANAGED, "fileset41", props));
    Assertions.assertEquals(
        "The FILESET name '*' is reserved. Illegal name: *", exception.getMessage());

    NameIdentifier filesetIdent2 = NameIdentifier.of(filesetNs, "a?");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                filesetNormalizeDispatcher.createFileset(
                    filesetIdent2, "comment", Fileset.Type.MANAGED, "fileset41", props));
    Assertions.assertEquals(
        "The FILESET name 'a?' is illegal. Illegal name: a?", exception.getMessage());
  }
}
