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

package org.apache.gravitino.cli.output;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.outputs.PlainFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPlainFormat {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  void setUp() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testMetalakeDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Metalake mockMetalake = getMockMetalake();

    PlainFormat.output(mockMetalake, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("demo_metalake, This is a demo metalake", output);
  }

  @Test
  void testListMetalakeWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Metalake mockMetalake1 = getMockMetalake("metalake1", "This is a metalake");
    Metalake mockMetalake2 = getMockMetalake("metalake2", "This is another metalake");

    PlainFormat.output(new Metalake[] {mockMetalake1, mockMetalake2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("metalake1\n" + "metalake2", output);
  }

  @Test
  void testCatalogDetailsWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog = getMockCatalog();

    PlainFormat.output(mockCatalog, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "demo_catalog, RELATIONAL, demo_provider, This is a demo catalog", output);
  }

  @Test
  void testListCatalogWithPlainFormat() {
    CommandContext mockContext = getMockContext();
    Catalog mockCatalog1 =
        getMockCatalog("catalog1", Catalog.Type.FILESET, "provider1", "This is a catalog");
    Catalog mockCatalog2 =
        getMockCatalog("catalog2", Catalog.Type.RELATIONAL, "provider2", "This is another catalog");

    PlainFormat.output(new Catalog[] {mockCatalog1, mockCatalog2}, mockContext);
    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals("catalog1\n" + "catalog2", output);
  }

  @Test
  void testOutputWithUnsupportType() {
    CommandContext mockContext = getMockContext();
    Object mockObject = new Object();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PlainFormat.output(mockObject, mockContext);
        });
  }

  private CommandContext getMockContext() {
    CommandContext mockContext = mock(CommandContext.class);

    return mockContext;
  }

  private Metalake getMockMetalake() {
    return getMockMetalake("demo_metalake", "This is a demo metalake");
  }

  private Metalake getMockMetalake(String name, String comment) {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn(name);
    when(mockMetalake.comment()).thenReturn(comment);

    return mockMetalake;
  }

  private Catalog getMockCatalog() {
    return getMockCatalog(
        "demo_catalog", Catalog.Type.RELATIONAL, "demo_provider", "This is a demo catalog");
  }

  private Catalog getMockCatalog(String name, Catalog.Type type, String provider, String comment) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comment);

    return mockCatalog;
  }

  private Schema getMockSchema() {
    return getMockSchema("demo_schema", "This is a demo schema");
  }

  private Schema getMockSchema(String name, String comment) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);

    return mockSchema;
  }

  private Audit getMockAudit() {
    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(mockAudit.lastModifier()).thenReturn("demo_user");
    when(mockAudit.lastModifiedTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));

    return mockAudit;
  }
}
