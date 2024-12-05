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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class TestParseType {

  @Test
  public void testParseVarcharWithLength() {
    ParsedType parsed = ParseType.parse("varchar(10)");
    assertNotNull(parsed);
    assertEquals("varchar", parsed.getTypeName());
    assertEquals(10, parsed.getLength());
    assertNull(parsed.getScale());
    assertNull(parsed.getPrecision());
  }

  @Test
  public void testParseDecimalWithPrecisionAndScale() {
    ParsedType parsed = ParseType.parse("decimal(10,5)");
    assertNotNull(parsed);
    assertEquals("decimal", parsed.getTypeName());
    assertEquals(10, parsed.getPrecision());
    assertEquals(5, parsed.getScale());
    assertNull(parsed.getLength());
  }

  @Test
  public void testParseIntegerWithoutParameters() {
    ParsedType parsed = ParseType.parse("int()");
    assertNull(parsed); // Expect null because the format is unsupported
  }

  @Test
  public void testParseOrdinaryInput() {
    assertNull(ParseType.parse("string"));
    assertNull(ParseType.parse("int"));
  }

  @Test
  public void testParseMalformedInput() {
    assertNull(ParseType.parse("varchar(-10)"));
    assertNull(ParseType.parse("decimal(10,abc)"));
  }
}
