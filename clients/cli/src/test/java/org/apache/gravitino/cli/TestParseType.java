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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestParseType {

  @Test
  public void testParseTypeVarcharWithLength() {
    Type type = ParseType.toType("varchar(10)");
    assertThat(type, instanceOf(Types.VarCharType.class));
    assertEquals(10, ((Types.VarCharType) type).length());
  }

  @Test
  public void testParseTypeDecimalWithPrecisionAndScale() {
    Type type = ParseType.toType("decimal(10,5)");
    assertThat(type, instanceOf(Types.DecimalType.class));
    assertEquals(10, ((Types.DecimalType) type).precision());
    assertEquals(5, ((Types.DecimalType) type).scale());
  }

  @Test
  public void testParseTypeListValidInput() {
    Type type = ParseType.toType("list(integer)");
    assertThat(type, instanceOf(Types.ListType.class));
    Type elementType = ((Types.ListType) type).elementType();
    assertThat(elementType, instanceOf(Types.IntegerType.class));
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
    assertThat(type, instanceOf(Types.MapType.class));
    Type keyType = ((Types.MapType) type).keyType();
    Type valueType = ((Types.MapType) type).valueType();
    assertThat(keyType, instanceOf(Types.StringType.class));
    assertThat(valueType, instanceOf(Types.IntegerType.class));
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
    assertThat(type, instanceOf(Types.ListType.class));
    Type elementType = ((Types.ListType) type).elementType();
    assertThat(elementType, instanceOf(Types.IntegerType.class));
  }

  @Test
  public void testParseTypeMapWithSpaces() {
    Type type = ParseType.toType("map( string , integer )");
    assertThat(type, instanceOf(Types.MapType.class));
    Type keyType = ((Types.MapType) type).keyType();
    Type valueType = ((Types.MapType) type).valueType();
    assertThat(keyType, instanceOf(Types.StringType.class));
    assertThat(valueType, instanceOf(Types.IntegerType.class));
  }

  @Test
  public void testParseTypeNestedList() {
    Type type = ParseType.toType("list(list(integer))");
    assertThat(type, instanceOf(Types.ListType.class));
    Type innerList = ((Types.ListType) type).elementType();
    assertThat(innerList, instanceOf(Types.ListType.class));
    Type elementType = ((Types.ListType) innerList).elementType();
    assertThat(elementType, instanceOf(Types.IntegerType.class));
  }

  @Test
  public void testParseTypeMapWithNestedValue() {
    Type type = ParseType.toType("map(string,list(integer))");
    assertThat(type, instanceOf(Types.MapType.class));
    Type keyType = ((Types.MapType) type).keyType();
    Type valueType = ((Types.MapType) type).valueType();
    assertThat(keyType, instanceOf(Types.StringType.class));
    assertThat(valueType, instanceOf(Types.ListType.class));
    Type elementType = ((Types.ListType) valueType).elementType();
    assertThat(elementType, instanceOf(Types.IntegerType.class));
  }

  @Test
  public void testParseTypeDecimalWithSpaces() {
    Type type = ParseType.toType("decimal(10, 5)");
    assertThat(type, instanceOf(Types.DecimalType.class));
    assertEquals(10, ((Types.DecimalType) type).precision());
    assertEquals(5, ((Types.DecimalType) type).scale());
  }

  @Test
  public void testParseTypeVarcharWithSpaces() {
    Type type = ParseType.toType("varchar( 10 )");
    assertThat(type, instanceOf(Types.VarCharType.class));
    assertEquals(10, ((Types.VarCharType) type).length());
  }
}
