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

/** Converts {@code restricted-rego-v1} source text into tokens. */
final class RestrictedRegoLexer {
  private final String source;
  private int offset;
  private int line = 1;
  private int column = 1;

  RestrictedRegoLexer(String source) {
    this.source = source;
  }

  Token nextToken() {
    skipWhitespace();
    int tokenOffset = offset;
    int tokenLine = line;
    int tokenColumn = column;
    if (atEnd()) {
      return new Token(TokenType.EOF, "", tokenLine, tokenColumn);
    }

    char current = peek();
    if (current == '"') {
      scanString(tokenLine, tokenColumn);
      return token(TokenType.STRING, tokenOffset, tokenLine, tokenColumn);
    }
    if (isIdentifierStart(current)) {
      do {
        advance();
      } while (!atEnd() && isIdentifierPart(peek()));
      String text = source.substring(tokenOffset, offset);
      return new Token(keywordType(text, tokenLine, tokenColumn), text, tokenLine, tokenColumn);
    }
    if (isDigit(current) || (current == '-' && isDigit(peekNext()))) {
      scanNumber(tokenLine, tokenColumn);
      if (offset - tokenOffset > RestrictedRegoLimits.MAX_NUMERIC_LITERAL_BYTES) {
        throw lexicalError(
            tokenLine,
            tokenColumn,
            "numeric literal must not exceed "
                + RestrictedRegoLimits.MAX_NUMERIC_LITERAL_BYTES
                + " bytes");
      }
      if (!atEnd() && isIdentifierStart(peek())) {
        throw lexicalError(
            tokenLine, tokenColumn, "numeric literal must be separated from identifiers");
      }
      return token(TokenType.NUMBER, tokenOffset, tokenLine, tokenColumn);
    }

    advance();
    switch (current) {
      case ':':
        return symbol(TokenType.ASSIGN, '=', tokenOffset, tokenLine, tokenColumn);
      case '=':
        return symbol(TokenType.EQ, '=', tokenOffset, tokenLine, tokenColumn);
      case '!':
        return symbol(TokenType.NEQ, '=', tokenOffset, tokenLine, tokenColumn);
      case '<':
        return optionalEquals(TokenType.LT, TokenType.LTE, tokenOffset, tokenLine, tokenColumn);
      case '>':
        return optionalEquals(TokenType.GT, TokenType.GTE, tokenOffset, tokenLine, tokenColumn);
      case '(':
        return token(TokenType.LPAREN, tokenOffset, tokenLine, tokenColumn);
      case ')':
        return token(TokenType.RPAREN, tokenOffset, tokenLine, tokenColumn);
      case '[':
        return token(TokenType.LBRACKET, tokenOffset, tokenLine, tokenColumn);
      case ']':
        return token(TokenType.RBRACKET, tokenOffset, tokenLine, tokenColumn);
      case ',':
        return token(TokenType.COMMA, tokenOffset, tokenLine, tokenColumn);
      default:
        throw lexicalError(tokenLine, tokenColumn, "unsupported character " + current);
    }
  }

  private void scanString(int tokenLine, int tokenColumn) {
    advance();
    while (!atEnd()) {
      char current = advance();
      if (current == '"') {
        return;
      }
      if (current < 0x20) {
        throw lexicalError(tokenLine, tokenColumn, "unescaped control character in string");
      }
      if (current != '\\') {
        continue;
      }
      if (atEnd()) {
        break;
      }
      char escaped = advance();
      if (escaped == 'u') {
        for (int index = 0; index < 4; index++) {
          if (atEnd() || !isAsciiHexDigit(advance())) {
            throw lexicalError(tokenLine, tokenColumn, "invalid Unicode escape in string");
          }
        }
      } else if ("\"\\/bfnrt".indexOf(escaped) < 0) {
        throw lexicalError(tokenLine, tokenColumn, "unsupported string escape");
      }
    }
    throw lexicalError(tokenLine, tokenColumn, "unterminated string");
  }

  private void scanNumber(int tokenLine, int tokenColumn) {
    if (peek() == '-') {
      advance();
    }
    if (peek() == '0') {
      advance();
      if (!atEnd() && isDigit(peek())) {
        throw lexicalError(tokenLine, tokenColumn, "numeric literals cannot contain leading zeros");
      }
    } else {
      while (!atEnd() && isDigit(peek())) {
        advance();
      }
    }
    if (!atEnd() && peek() == '.' && isDigit(peekNext())) {
      advance();
      while (!atEnd() && isDigit(peek())) {
        advance();
      }
    }
  }

  private Token symbol(
      TokenType type, char second, int tokenOffset, int tokenLine, int tokenColumn) {
    if (atEnd() || peek() != second) {
      throw lexicalError(tokenLine, tokenColumn, "unsupported operator");
    }
    advance();
    return token(type, tokenOffset, tokenLine, tokenColumn);
  }

  private Token optionalEquals(
      TokenType single, TokenType withEquals, int tokenOffset, int tokenLine, int tokenColumn) {
    TokenType type = single;
    if (!atEnd() && peek() == '=') {
      advance();
      type = withEquals;
    }
    return token(type, tokenOffset, tokenLine, tokenColumn);
  }

  private Token token(TokenType type, int tokenOffset, int tokenLine, int tokenColumn) {
    return new Token(type, source.substring(tokenOffset, offset), tokenLine, tokenColumn);
  }

  private void skipWhitespace() {
    while (!atEnd()) {
      char current = peek();
      if (current != ' ' && current != '\t' && current != '\r' && current != '\n') {
        return;
      }
      advance();
    }
  }

  private char advance() {
    char current = source.charAt(offset++);
    if (current == '\n') {
      line++;
      column = 1;
    } else {
      column++;
    }
    return current;
  }

  private char peek() {
    return source.charAt(offset);
  }

  private char peekNext() {
    return offset + 1 < source.length() ? source.charAt(offset + 1) : '\0';
  }

  private boolean atEnd() {
    return offset >= source.length();
  }

  private static TokenType keywordType(String text, int line, int column) {
    switch (text) {
      case "filter":
        return TokenType.FILTER;
      case "mask":
        return TokenType.MASK;
      case "if":
        return TokenType.IF;
      case "else":
        return TokenType.ELSE;
      case "or":
        return TokenType.OR;
      case "and":
        return TokenType.AND;
      case "not":
        return TokenType.NOT;
      case "in":
        return TokenType.IN;
      case "col":
        return TokenType.COL;
      case "session_user":
        return TokenType.SESSION_USER;
      case "is_group_member":
        return TokenType.IS_GROUP_MEMBER;
      case "action":
        return TokenType.ACTION;
      case "true":
        return TokenType.TRUE;
      case "false":
        return TokenType.FALSE;
      case "null":
        return TokenType.NULL;
      default:
        throw lexicalError(line, column, "unsupported identifier " + text);
    }
  }

  private static boolean isIdentifierStart(char value) {
    return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || value == '_';
  }

  private static boolean isIdentifierPart(char value) {
    return isIdentifierStart(value) || isDigit(value);
  }

  private static boolean isDigit(char value) {
    return value >= '0' && value <= '9';
  }

  static boolean isAsciiHexDigit(char value) {
    return (value >= '0' && value <= '9')
        || (value >= 'a' && value <= 'f')
        || (value >= 'A' && value <= 'F');
  }

  static int asciiHexValue(char value) {
    if (value >= '0' && value <= '9') {
      return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
      return value - 'a' + 10;
    }
    return value - 'A' + 10;
  }

  private static IllegalArgumentException lexicalError(int line, int column, String message) {
    return new IllegalArgumentException(
        String.format(
            "Invalid restricted-rego-v1 program at line %s, column %s: %s", line, column, message));
  }

  enum TokenType {
    FILTER,
    MASK,
    IF,
    ELSE,
    OR,
    AND,
    NOT,
    IN,
    COL,
    SESSION_USER,
    IS_GROUP_MEMBER,
    ACTION,
    TRUE,
    FALSE,
    NULL,
    ASSIGN,
    EQ,
    NEQ,
    LTE,
    LT,
    GTE,
    GT,
    LPAREN,
    RPAREN,
    LBRACKET,
    RBRACKET,
    COMMA,
    NUMBER,
    STRING,
    EOF
  }

  static final class Token {
    final TokenType type;
    final String text;
    final int line;
    final int column;

    private Token(TokenType type, String text, int line, int column) {
      this.type = type;
      this.text = text;
      this.line = line;
      this.column = column;
    }
  }
}
