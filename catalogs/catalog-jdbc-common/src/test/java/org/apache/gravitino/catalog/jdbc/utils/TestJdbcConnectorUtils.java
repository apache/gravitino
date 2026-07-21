/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.jdbc.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcConnectorUtils {

  @Test
  public void testEscapeSqlLiteral() {
    String value = "owner\\'s \"comment\"; DROP TABLE marker; --";

    Assertions.assertEquals(
        "owner\\\\''s \"comment\"; DROP TABLE marker; --",
        JdbcConnectorUtils.escapeSqlLiteral(value, '\''));
    Assertions.assertEquals(
        "owner\\\\'s \"\"comment\"\"; DROP TABLE marker; --",
        JdbcConnectorUtils.escapeSqlLiteral(value, '"'));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JdbcConnectorUtils.escapeSqlLiteral(value, '`'));
    Assertions.assertEquals("null", JdbcConnectorUtils.escapeSqlLiteral(null, '\''));
  }

  @Test
  public void testUnescapeSqlLiteral() {
    String value = "owner\\'s \"comment\"; DROP TABLE marker; --";

    // Round-trip: unescape(escape(x)) == x for both quote styles.
    Assertions.assertEquals(
        value,
        JdbcConnectorUtils.unescapeSqlLiteral(
            JdbcConnectorUtils.escapeSqlLiteral(value, '\''), '\''));
    Assertions.assertEquals(
        value,
        JdbcConnectorUtils.unescapeSqlLiteral(
            JdbcConnectorUtils.escapeSqlLiteral(value, '"'), '"'));

    // Backslash-style escaping is unescaped too.
    Assertions.assertEquals(
        "owner's \"comment\"",
        JdbcConnectorUtils.unescapeSqlLiteral("owner's \\\"comment\\\"", '"'));

    // Text that was never escaped passes through unchanged, including a lone backslash before an
    // ordinary character and a lone trailing backslash.
    Assertions.assertEquals(
        "D:\\data; --", JdbcConnectorUtils.unescapeSqlLiteral("D:\\data; --", '"'));
    Assertions.assertEquals("tail\\", JdbcConnectorUtils.unescapeSqlLiteral("tail\\", '"'));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JdbcConnectorUtils.unescapeSqlLiteral(value, '`'));
  }
}
