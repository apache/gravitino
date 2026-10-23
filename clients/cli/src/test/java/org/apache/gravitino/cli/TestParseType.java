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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestParseType {

  @Test
  public void testParseTypeVarcharWithLength() {
    Type type = ParseType.toType("varchar(10)");
    assertInstanceOf(Types.VarCharType.class, type);
    assertEquals(10, ((Types.VarCharType) type).length());
  }

  @Test
  public void testParseTypeDecimalWithPrecisionAndScale() {
    Type type = ParseType.toType("decimal(10,5)");
    assertInstanceOf(Types.DecimalType.class, type);
    assertEquals(10, ((Types.DecimalType) type).precision());
    assertEquals(5, ((Types.DecimalType) type).scale());
  }

  @Test
  public void testParseTypeVariant() {
    Type type = ParseType.toType("variant");
    assertInstanceOf(Types.VariantType.class, type);
  }

  @Test
  public void testParseTypeListValidInput() {
    Type type = ParseType.toType("list(integer)");
    assertInstanceOf(Types.ListType.class, type);
    Type elementType = ((Types.ListType) type).elementType();
    assertInstanceOf(Types.IntegerType.class, elementType);
  }

  @Test
  public void testParseTypeListMalformedInput() {
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("list()"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("list(10)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("list(unknown)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("list(integer,integer)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("list(integer"));
  }

  @Test
  public void testParseTypeMapValidInput() {
    Type type = ParseType.toType("map(string,integer)");
    assertInstanceOf(Types.MapType.class, type);
    Type keyType = ((Types.MapType) type).keyType();
    Type valueType = ((Types.MapType) type).valueType();
    assertInstanceOf(Types.StringType.class, keyType);
    assertInstanceOf(Types.IntegerType.class, valueType);
  }

  @Test
  public void testParseTypeMapMalformedInput() {
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("map()"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("map(10,10)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("map(unknown,unknown)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("map(string)"));
    assertThrows(
        IllegalArgumentException.class, () -> ParseType.toType("map(string,integer,integer)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("map(string,integer"));
  }

  @Test
  public void testParseTypeIntegerWithoutParameters() {
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("int()"));
  }

  @Test
  public void testParseTypeOrdinaryInput() {
    assertNull(ParseType.parseBasicType("string"));
    assertNull(ParseType.parseBasicType("int"));
  }

  @Test
  public void testParseTypeMalformedInput() {
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("varchar(-10)"));
    assertThrows(IllegalArgumentException.class, () -> ParseType.toType("decimal(10,abc)"));
  }

  @Test
  public void testParseTypeListWithSpaces() {
    Type type = ParseType.toType("list( integer )");
    assertInstanceOf(Types.ListType.class, type);
    Type elementType = ((Types.ListType) type).elementType();
    assertInstanceOf(Types.IntegerType.class, elementType);
  }

  @Test
  public void testParseTypeMapWithSpaces() {
    Type type = ParseType.toType("map( string , integer )");
    assertInstanceOf(Types.MapType.class, type);
    Type keyType = ((Types.MapType) type).keyType();
    Type valueType = ((Types.MapType) type).valueType();
    assertInstanceOf(Types.StringType.class, keyType);
    assertInstanceOf(Types.IntegerType.class, valueType);
  }

  @Test
  public void testParseTypeNestedList() {
    Type type = ParseType.toType("list(list(integer))");
    assertInstanceOf(Types.ListType.class, type);
    Type innerList = ((Types.ListType) type).elementType();
    assertInstanceOf(Types.ListType.class, innerList);
    Type elementType = ((Types.ListType) innerList).elementType();
    assertInstanceOf(Types.IntegerType.class, elementType);
  }

  @Test
  public void testParseTypeMapWithNestedValue() {
    Type type = ParseType.toType("map(string,list(integer))");
    assertInstanceOf(Types.MapType.class, type);
    Type keyType = ((Types.MapType) type).keyType();
    Type valueType = ((Types.MapType) type).valueType();
    assertInstanceOf(Types.StringType.class, keyType);
    assertInstanceOf(Types.ListType.class, valueType);
    Type elementType = ((Types.ListType) valueType).elementType();
    assertInstanceOf(Types.IntegerType.class, elementType);
  }

  @Test
  public void testParseTypeDecimalWithSpaces() {
    Type type = ParseType.toType("decimal(10, 5)");
    assertInstanceOf(Types.DecimalType.class, type);
    assertEquals(10, ((Types.DecimalType) type).precision());
    assertEquals(5, ((Types.DecimalType) type).scale());
  }

  @Test
  public void testParseTypeVarcharWithSpaces() {
    Type type = ParseType.toType("varchar( 10 )");
    assertInstanceOf(Types.VarCharType.class, type);
    assertEquals(10, ((Types.VarCharType) type).length());
  }

  /** Ensures that "decimal(10)" is parsed as DecimalType with precision=10 and scale=0. */
  @Test
  public void testParseTypeDecimalWithPrecisionOnly() {
    Type type = ParseType.toType("decimal(10)");
    assertInstanceOf(Types.DecimalType.class, type);
    assertEquals(10, ((Types.DecimalType) type).precision());
    assertEquals(0, ((Types.DecimalType) type).scale());
  }

  /** Accepts case-insensitive input and extra spaces for the single-arg form. */
  @Test
  public void testParseTypeDecimalWithPrecisionOnlySpacesAndCase() {
    Type type = ParseType.toType("DECIMAL ( 10 ) ");
    assertInstanceOf(Types.DecimalType.class, type);
    assertEquals(10, ((Types.DecimalType) type).precision());
    assertEquals(0, ((Types.DecimalType) type).scale());
  }
}
