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
package org.apache.gravitino.client.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Verifies that names and comments longer than their metadata store columns are rejected with a
 * clear error instead of a database error, on every metadata store backend.
 */
public class NameAndCommentLengthIT extends BaseIT {

  private static final String TOO_LONG_NAME = StringUtils.repeat("n", 129);
  private static final String MAX_LENGTH_NAME = StringUtils.repeat("n", 128);
  private static final String TOO_LONG_COMMENT = StringUtils.repeat("c", 257);
  private static final String MAX_LENGTH_COMMENT = StringUtils.repeat("c", 256);

  private final String metalakeName = GravitinoITUtils.genRandomName("length_it_metalake");
  private GravitinoMetalake metalake;
  private File localStorage;

  @BeforeAll
  public void setUp() throws IOException {
    metalake = client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    localStorage = Files.createTempDirectory("length_it_storage").toFile();
  }

  @AfterAll
  public void tearDown() throws IOException {
    client.dropMetalake(metalakeName, true);
    FileUtils.deleteDirectory(localStorage);
  }

  @Test
  public void testTagNameAndCommentLength() {
    assertTooLong(
        "The name of the tag must not exceed 128 characters",
        () -> metalake.createTag(TOO_LONG_NAME, "comment", Collections.emptyMap()));

    String tagName = GravitinoITUtils.genRandomName("length_it_tag");
    assertTooLong(
        "The comment of the tag must not exceed 256 characters",
        () -> metalake.createTag(tagName, TOO_LONG_COMMENT, Collections.emptyMap()));
    Assertions.assertThrows(NoSuchTagException.class, () -> metalake.getTag(tagName));

    Tag tag = metalake.createTag(MAX_LENGTH_NAME, MAX_LENGTH_COMMENT, Collections.emptyMap());
    Assertions.assertEquals(MAX_LENGTH_NAME, tag.name());
    Assertions.assertEquals(MAX_LENGTH_COMMENT, tag.comment());

    assertTooLong(
        "The name of the tag must not exceed 128 characters",
        () -> metalake.alterTag(MAX_LENGTH_NAME, TagChange.rename(TOO_LONG_NAME)));
    assertTooLong(
        "The comment of the tag must not exceed 256 characters",
        () -> metalake.alterTag(MAX_LENGTH_NAME, TagChange.updateComment(TOO_LONG_COMMENT)));
    Assertions.assertEquals(MAX_LENGTH_COMMENT, metalake.getTag(MAX_LENGTH_NAME).comment());

    Assertions.assertTrue(metalake.deleteTag(MAX_LENGTH_NAME));
  }

  @Test
  public void testPolicyNameLength() {
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"), ImmutableSet.of(MetadataObject.Type.TABLE), null);

    assertTooLong(
        "The name of the policy must not exceed 128 characters",
        () -> metalake.createPolicy(TOO_LONG_NAME, "custom", "comment", true, content));

    Policy policy = metalake.createPolicy(MAX_LENGTH_NAME, "custom", "comment", true, content);
    Assertions.assertEquals(MAX_LENGTH_NAME, policy.name());

    assertTooLong(
        "The name of the policy must not exceed 128 characters",
        () -> metalake.alterPolicy(MAX_LENGTH_NAME, PolicyChange.rename(TOO_LONG_NAME)));
    Assertions.assertEquals(MAX_LENGTH_NAME, metalake.getPolicy(MAX_LENGTH_NAME).name());

    Assertions.assertTrue(metalake.deletePolicy(MAX_LENGTH_NAME));
  }

  @Test
  public void testCatalogCommentLength() {
    String catalogName = GravitinoITUtils.genRandomName("length_it_catalog");

    assertTooLong(
        "The comment of the catalog must not exceed 256 characters",
        () -> createFilesetCatalog(catalogName, TOO_LONG_COMMENT));
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Catalog catalog = createFilesetCatalog(catalogName, MAX_LENGTH_COMMENT);
    Assertions.assertEquals(MAX_LENGTH_COMMENT, catalog.comment());

    assertTooLong(
        "The comment of the catalog must not exceed 256 characters",
        () -> metalake.alterCatalog(catalogName, CatalogChange.updateComment(TOO_LONG_COMMENT)));
    Assertions.assertEquals(MAX_LENGTH_COMMENT, metalake.loadCatalog(catalogName).comment());

    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testSchemaAndFilesetCommentLength() {
    String catalogName = GravitinoITUtils.genRandomName("length_it_fileset_catalog");
    Catalog catalog = createFilesetCatalog(catalogName, "comment");

    // A rejected schema must not leave its directory behind.
    String schemaName = GravitinoITUtils.genRandomName("length_it_schema");
    assertTooLong(
        "The comment of the schema must not exceed 256 characters",
        () -> catalog.asSchemas().createSchema(schemaName, TOO_LONG_COMMENT, null));
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schemaName));
    Assertions.assertFalse(new File(localStorage, catalogName + "/" + schemaName).exists());

    catalog.asSchemas().createSchema(schemaName, MAX_LENGTH_COMMENT, null);
    Assertions.assertTrue(new File(localStorage, catalogName + "/" + schemaName).exists());
    Assertions.assertEquals(
        MAX_LENGTH_COMMENT, catalog.asSchemas().loadSchema(schemaName).comment());

    // A rejected fileset must not leave its directory behind.
    NameIdentifier filesetIdent =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("length_it_fileset"));
    assertTooLong(
        "The comment of the fileset must not exceed 256 characters",
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(filesetIdent, TOO_LONG_COMMENT, Fileset.Type.MANAGED, null, null));
    Assertions.assertFalse(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    Assertions.assertFalse(
        new File(localStorage, catalogName + "/" + schemaName + "/" + filesetIdent.name())
            .exists());

    Fileset fileset =
        catalog
            .asFilesetCatalog()
            .createFileset(filesetIdent, MAX_LENGTH_COMMENT, Fileset.Type.MANAGED, null, null);
    Assertions.assertEquals(MAX_LENGTH_COMMENT, fileset.comment());
    Assertions.assertTrue(
        new File(localStorage, catalogName + "/" + schemaName + "/" + filesetIdent.name())
            .exists());

    assertTooLong(
        "The comment of the fileset must not exceed 256 characters",
        () ->
            catalog
                .asFilesetCatalog()
                .alterFileset(filesetIdent, FilesetChange.updateComment(TOO_LONG_COMMENT)));
    Assertions.assertEquals(
        MAX_LENGTH_COMMENT, catalog.asFilesetCatalog().loadFileset(filesetIdent).comment());

    metalake.dropCatalog(catalogName, true);
  }

  private Catalog createFilesetCatalog(String catalogName, String comment) {
    String location = localStorage.toURI() + catalogName;
    return metalake.createCatalog(
        catalogName,
        Catalog.Type.FILESET,
        "hadoop",
        comment,
        ImmutableMap.of("location", location));
  }

  private static void assertTooLong(String expectedMessage, Executable executable) {
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, executable);
    Assertions.assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }
}
