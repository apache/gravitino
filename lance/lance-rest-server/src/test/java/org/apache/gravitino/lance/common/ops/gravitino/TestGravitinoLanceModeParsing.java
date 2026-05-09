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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_LOCATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InvalidInputException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestGravitinoLanceModeParsing {

  private enum TestMode {
    CREATE,
    EXIST_OK
  }

  @Test
  void testNormalizeTokenPreservesSpecialCharacters() {
    Assertions.assertEquals("CREATE", CommonUtil.normalizeToken(" create "));
    Assertions.assertEquals("EXIST_OK", CommonUtil.normalizeToken("exist_ok"));
    Assertions.assertEquals("#CREATE$", CommonUtil.normalizeToken("#create$"));
  }

  @Test
  void testParseEnumTokenRejectsMalformedValues() {
    Assertions.assertEquals(
        TestMode.EXIST_OK,
        CommonUtil.parseEnumToken(TestMode.class, "exist_ok", "Unknown mode: ", "table"));

    InvalidInputException exception =
        Assertions.assertThrows(
            InvalidInputException.class,
            () -> CommonUtil.parseEnumToken(TestMode.class, "#create$", "Unknown mode: ", "table"));
    Assertions.assertTrue(exception.getMessage().contains("Unknown mode: #create$"));
  }

  @Test
  void testNamespaceModeRejectsMalformedValues() {
    GravitinoLanceNameSpaceOperations operations =
        new GravitinoLanceNameSpaceOperations(Mockito.mock(GravitinoLanceNamespaceWrapper.class));
    String delimiter = Pattern.quote(".");

    InvalidInputException createException =
        Assertions.assertThrows(
            InvalidInputException.class,
            () -> operations.createNamespace("catalog", delimiter, "#create$", Map.of()));
    Assertions.assertTrue(
        createException.getMessage().contains("Unknown create namespace mode: #create$"));

    InvalidInputException dropModeException =
        Assertions.assertThrows(
            InvalidInputException.class,
            () -> operations.dropNamespace("catalog", delimiter, "#fail$", "restrict"));
    Assertions.assertTrue(
        dropModeException.getMessage().contains("Unknown drop namespace mode: #fail$"));

    InvalidInputException dropBehaviorException =
        Assertions.assertThrows(
            InvalidInputException.class,
            () -> operations.dropNamespace("catalog", delimiter, "fail", "#cascade$"));
    Assertions.assertTrue(
        dropBehaviorException.getMessage().contains("Unknown drop namespace behavior: #cascade$"));
  }

  @Test
  void testCreateTableModeNormalizesCase() {
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Table table = Mockito.mock(Table.class);
    when(table.properties()).thenReturn(Map.of());
    when(tableCatalog.createTable(
            any(NameIdentifier.class), any(Column[].class), isNull(), anyMap()))
        .thenReturn(table);
    GravitinoLanceTableOperations operations = newTableOperations(tableCatalog);

    operations.createTable("catalog.schema.table", " exist_ok ", ".", null, Map.of(), null);

    ArgumentCaptor<Map<String, String>> propertiesCaptor = propertiesCaptor();
    Mockito.verify(tableCatalog)
        .createTable(
            any(NameIdentifier.class), any(Column[].class), isNull(), propertiesCaptor.capture());
    Assertions.assertEquals("EXIST_OK", propertiesCaptor.getValue().get(LANCE_CREATION_MODE));
  }

  @Test
  void testCreateTableModeRejectsMalformedValues() {
    GravitinoLanceTableOperations operations = newTableOperations(Mockito.mock(TableCatalog.class));

    InvalidInputException exception =
        Assertions.assertThrows(
            InvalidInputException.class,
            () ->
                operations.createTable(
                    "catalog.schema.table", "#create$", ".", null, Map.of(), null));
    Assertions.assertTrue(exception.getMessage().contains("Unknown create table mode: #create$"));
  }

  @Test
  void testRegisterModeMapsRegisterToCreate() {
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Table table = Mockito.mock(Table.class);
    when(table.properties()).thenReturn(Map.of(LANCE_LOCATION, "/tmp/table"));
    when(tableCatalog.createTable(
            any(NameIdentifier.class), any(Column[].class), isNull(), anyMap()))
        .thenReturn(table);
    GravitinoLanceTableOperations operations = newTableOperations(tableCatalog);

    operations.registerTable(
        "catalog.schema.table", " register ", ".", Map.of(LANCE_LOCATION, "/tmp/table"));

    ArgumentCaptor<Map<String, String>> propertiesCaptor = propertiesCaptor();
    Mockito.verify(tableCatalog)
        .createTable(
            any(NameIdentifier.class), any(Column[].class), isNull(), propertiesCaptor.capture());
    Assertions.assertEquals("CREATE", propertiesCaptor.getValue().get(LANCE_CREATION_MODE));
  }

  @Test
  void testRegisterModeRejectsMalformedValues() {
    GravitinoLanceTableOperations operations = newTableOperations(Mockito.mock(TableCatalog.class));

    InvalidInputException exception =
        Assertions.assertThrows(
            InvalidInputException.class,
            () ->
                operations.registerTable(
                    "catalog.schema.table",
                    "#register$",
                    ".",
                    Map.of(LANCE_LOCATION, "/tmp/table")));
    Assertions.assertTrue(
        exception.getMessage().contains("Unknown register table mode: #register$"));
  }

  private static GravitinoLanceTableOperations newTableOperations(TableCatalog tableCatalog) {
    GravitinoLanceNamespaceWrapper namespaceWrapper =
        Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    when(namespaceWrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    when(catalog.asTableCatalog()).thenReturn(tableCatalog);
    return new GravitinoLanceTableOperations(namespaceWrapper);
  }

  @SuppressWarnings("unchecked")
  private static ArgumentCaptor<Map<String, String>> propertiesCaptor() {
    return ArgumentCaptor.forClass(Map.class);
  }
}
