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
package org.apache.gravitino.policy.rego;

import org.apache.gravitino.policy.rego.RestrictedRegoLexer.Token;
import org.apache.gravitino.policy.rego.RestrictedRegoLexer.TokenType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link RestrictedRegoLexer}. */
public class TestRestrictedRegoLexer {

  @Test
  void testTokenizesSourceWithOneBasedLocations() {
    RestrictedRegoLexer lexer = new RestrictedRegoLexer("filter :=\n col(\"x\") >= -1");

    assertToken(lexer.nextToken(), TokenType.FILTER, "filter", 1, 1);
    assertToken(lexer.nextToken(), TokenType.ASSIGN, ":=", 1, 8);
    assertToken(lexer.nextToken(), TokenType.COL, "col", 2, 2);
    assertToken(lexer.nextToken(), TokenType.LPAREN, "(", 2, 5);
    assertToken(lexer.nextToken(), TokenType.STRING, "\"x\"", 2, 6);
    assertToken(lexer.nextToken(), TokenType.RPAREN, ")", 2, 9);
    assertToken(lexer.nextToken(), TokenType.GTE, ">=", 2, 11);
    assertToken(lexer.nextToken(), TokenType.NUMBER, "-1", 2, 14);
    assertToken(lexer.nextToken(), TokenType.EOF, "", 2, 16);
  }

  @Test
  void testRejectsInvalidUnicodeEscapeDigits() {
    for (String invalidEscape : RestrictedRegoTestUtils.invalidUnicodeEscapes()) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new RestrictedRegoLexer("\"" + invalidEscape + "\"").nextToken());
      Assertions.assertTrue(error.getMessage().contains("invalid Unicode escape"));
    }
  }

  @Test
  void testRejectsMalformedTokensAtTheirStart() {
    IllegalArgumentException identifierError =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new RestrictedRegoLexer("region").nextToken());
    Assertions.assertTrue(identifierError.getMessage().contains("line 1, column 1"));
    Assertions.assertTrue(identifierError.getMessage().contains("unsupported identifier region"));

    IllegalArgumentException leadingZeroError =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new RestrictedRegoLexer("01").nextToken());
    Assertions.assertTrue(leadingZeroError.getMessage().contains("leading zeros"));

    IllegalArgumentException largeNumberError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new RestrictedRegoLexer("1" + "0".repeat(256)).nextToken());
    Assertions.assertTrue(largeNumberError.getMessage().contains("must not exceed 256 bytes"));
  }

  private static void assertToken(Token token, TokenType type, String text, int line, int column) {
    Assertions.assertEquals(type, token.type);
    Assertions.assertEquals(text, token.text);
    Assertions.assertEquals(line, token.line);
    Assertions.assertEquals(column, token.column);
  }
}
