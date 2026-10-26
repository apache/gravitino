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
package org.apache.gravitino.policy.sql;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** Tokenizes SQL without interpreting backslashes or accepting comments and statements. */
final class RestrictedSqlLexer {
  private final String source;
  private int offset;

  RestrictedSqlLexer(String source) {
    this.source = source;
    for (int i = 0; i < source.length(); i++) {
      char c = source.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (++i >= source.length() || !Character.isLowSurrogate(source.charAt(i))) {
          throw error("isolated surrogate");
        }
      } else if (Character.isLowSurrogate(c) || c == 0) {
        throw error("invalid Unicode character");
      }
    }
  }

  Token next() {
    while (offset < source.length() && Character.isWhitespace(source.charAt(offset))) {
      offset++;
    }
    if (offset == source.length()) {
      return new Token("EOF", "");
    }
    char c = source.charAt(offset++);
    if (c == '\'' || c == '"') {
      StringBuilder value = new StringBuilder();
      boolean closed = false;
      while (offset < source.length()) {
        char next = source.charAt(offset++);
        if (next == c) {
          if (offset < source.length() && source.charAt(offset) == c) {
            offset++;
          } else {
            closed = true;
            break;
          }
        }
        value.append(next);
      }
      if (!closed) {
        throw error("unterminated quoted token");
      }
      if (value.toString().getBytes(StandardCharsets.UTF_8).length > 4096) {
        throw error("quoted token exceeds 4096 UTF-8 bytes");
      }
      return new Token(c == '\'' ? "STRING" : "QUOTED", value.toString());
    }
    int start = offset - 1;
    if (letter(c)) {
      while (offset < source.length()
          && (letter(source.charAt(offset)) || digit(source.charAt(offset)))) {
        offset++;
      }
      return new Token("WORD", source.substring(start, offset).toUpperCase(Locale.ROOT));
    }
    if (digit(c) || (c == '-' && offset < source.length() && digit(source.charAt(offset)))) {
      while (offset < source.length() && digit(source.charAt(offset))) {
        offset++;
      }
      if (offset < source.length() && source.charAt(offset) == '.') {
        offset++;
        while (offset < source.length() && digit(source.charAt(offset))) {
          offset++;
        }
      }
      String number = source.substring(start, offset);
      if (number.length() > 256 || !number.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?")) {
        throw error("invalid numeric literal or numeric literal exceeds 256 bytes");
      }
      if (offset < source.length() && letter(source.charAt(offset))) {
        throw error("numeric literal must end at a token boundary");
      }
      return new Token("NUMBER", number);
    }
    if (offset < source.length()) {
      String pair = source.substring(start, offset + 1);
      if (pair.matches("->|<>|!=|<=|>=")) {
        offset++;
        return new Token(pair, pair);
      }
    }
    if ("(),=<>".indexOf(c) >= 0) {
      return new Token(String.valueOf(c), String.valueOf(c));
    }
    throw error("unsupported character " + c);
  }

  IllegalArgumentException error(String message) {
    int line = 1;
    int column = 1;
    for (int i = 0; i < offset; i++) {
      if (source.charAt(i) == '\n') {
        line++;
        column = 1;
      } else {
        column++;
      }
    }
    return new IllegalArgumentException(
        "Invalid restricted-sql-v1 definition at line "
            + line
            + ", column "
            + column
            + ": "
            + message);
  }

  private static boolean letter(char c) {
    return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_';
  }

  private static boolean digit(char c) {
    return c >= '0' && c <= '9';
  }

  static final class Token {
    final String kind;
    final String text;

    Token(String kind, String text) {
      this.kind = kind;
      this.text = text;
    }

    boolean is(String expected) {
      return kind.equals(expected) || kind.equals("WORD") && text.equals(expected);
    }
  }
}
