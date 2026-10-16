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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.policy.rego.RestrictedRegoLexer.Token;
import org.apache.gravitino.policy.rego.RestrictedRegoLexer.TokenType;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.FilterBranch;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskBranch;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;

/** Builds a {@code restricted-rego-v1} program from lexer tokens. */
final class RestrictedRegoSourceParser {
  private final RestrictedRegoLexer lexer;
  private Token current;
  private int parenthesisNesting;

  RestrictedRegoSourceParser(String source) {
    this.lexer = new RestrictedRegoLexer(source);
    this.current = lexer.nextToken();
  }

  RestrictedRegoProgram parseProgram() {
    RestrictedRegoProgram program;
    if (check(TokenType.FILTER)) {
      program = parseRowFilter();
    } else if (check(TokenType.MASK)) {
      program = parseColumnMask();
    } else {
      throw error("expected filter or mask rule");
    }
    expect(TokenType.EOF, "expected end of program");
    return program;
  }

  private RowFilter parseRowFilter() {
    expect(TokenType.FILTER, "expected filter");
    expect(TokenType.ASSIGN, "expected := after filter");
    CanonicalExpression result = parseExpression();
    if (!match(TokenType.IF)) {
      return new RowFilter(new ArrayList<>(), result);
    }

    List<FilterBranch> branches = new ArrayList<>();
    branches.add(new FilterBranch(result, parseExpression()));
    while (true) {
      expect(TokenType.ELSE, "expected final else branch");
      expect(TokenType.ASSIGN, "expected := after else");
      CanonicalExpression nextResult = parseExpression();
      if (!match(TokenType.IF)) {
        return new RowFilter(branches, nextResult);
      }
      branches.add(new FilterBranch(nextResult, parseExpression()));
    }
  }

  private ColumnMask parseColumnMask() {
    expect(TokenType.MASK, "expected mask");
    expect(TokenType.ASSIGN, "expected := after mask");
    MaskAction action = parseMaskAction();
    if (!match(TokenType.IF)) {
      return new ColumnMask(new ArrayList<>(), action);
    }

    List<MaskBranch> branches = new ArrayList<>();
    branches.add(new MaskBranch(action, parseExpression()));
    while (true) {
      expect(TokenType.ELSE, "expected final else branch");
      expect(TokenType.ASSIGN, "expected := after else");
      MaskAction nextAction = parseMaskAction();
      if (!match(TokenType.IF)) {
        return new ColumnMask(branches, nextAction);
      }
      branches.add(new MaskBranch(nextAction, parseExpression()));
    }
  }

  private MaskAction parseMaskAction() {
    expect(TokenType.ACTION, "expected action");
    expect(TokenType.LPAREN, "expected ( after action");
    Token actionToken = expect(TokenType.STRING, "expected mask action string");
    String action = decodeString(actionToken);
    expect(TokenType.RPAREN, "expected ) after mask action");
    try {
      return MaskAction.fromValue(action);
    } catch (IllegalArgumentException exception) {
      throw error(actionToken, exception.getMessage());
    }
  }

  private CanonicalExpression parseExpression() {
    return parseOrExpression();
  }

  private CanonicalExpression parseOrExpression() {
    List<CanonicalExpression> operands = new ArrayList<>();
    operands.add(parseAndExpression());
    while (match(TokenType.OR)) {
      operands.add(parseAndExpression());
    }
    return LogicalExpression.of(LogicalOperator.OR, operands);
  }

  private CanonicalExpression parseAndExpression() {
    List<CanonicalExpression> operands = new ArrayList<>();
    operands.add(parseNotExpression());
    while (match(TokenType.AND)) {
      operands.add(parseNotExpression());
    }
    return LogicalExpression.of(LogicalOperator.AND, operands);
  }

  private CanonicalExpression parseNotExpression() {
    int notCount = 0;
    while (match(TokenType.NOT)) {
      notCount++;
      if (notCount >= RestrictedRegoLimits.MAX_SOURCE_DEPTH) {
        throw error(
            "restricted-rego-v1 source depth must not exceed "
                + RestrictedRegoLimits.MAX_SOURCE_DEPTH);
      }
    }

    CanonicalExpression result = parseComparisonExpression();
    for (int index = 0; index < notCount; index++) {
      result = new Not(result);
    }
    return result;
  }

  private CanonicalExpression parseComparisonExpression() {
    CanonicalExpression left = parsePrimary();
    if (!isComparisonOperator(current.type)) {
      return left;
    }

    ComparisonOperator operator = ComparisonOperator.fromSourceToken(advance().text);
    CanonicalExpression right = parsePrimary();
    if (isComparisonOperator(current.type)) {
      throw error("chained comparisons are not supported");
    }
    return new Comparison(operator, left, right);
  }

  private CanonicalExpression parsePrimary() {
    if (match(TokenType.COL)) {
      expect(TokenType.LPAREN, "expected ( after col");
      String column = decodeString(expect(TokenType.STRING, "expected column name string"));
      expect(TokenType.RPAREN, "expected ) after column name");
      return new Column(column);
    }
    if (match(TokenType.SESSION_USER)) {
      expect(TokenType.LPAREN, "expected ( after session_user");
      expect(TokenType.RPAREN, "expected ) after session_user(");
      return new SessionUser();
    }
    if (match(TokenType.IS_GROUP_MEMBER)) {
      expect(TokenType.LPAREN, "expected ( after is_group_member");
      String group = decodeString(expect(TokenType.STRING, "expected group name string"));
      expect(TokenType.RPAREN, "expected ) after group name");
      return new GroupMembership(group);
    }
    if (isLiteral(current.type)) {
      return parseLiteral();
    }
    if (match(TokenType.LBRACKET)) {
      return parseLiteralArray();
    }
    if (match(TokenType.LPAREN)) {
      parenthesisNesting++;
      if (parenthesisNesting > RestrictedRegoLimits.MAX_PARENTHESIS_NESTING) {
        throw error(
            "parenthesis nesting must not exceed " + RestrictedRegoLimits.MAX_PARENTHESIS_NESTING);
      }
      CanonicalExpression expression = parseExpression();
      expect(TokenType.RPAREN, "expected ) after expression");
      parenthesisNesting--;
      return expression;
    }
    throw error("expected expression");
  }

  private Literal parseLiteral() {
    Token token = advance();
    switch (token.type) {
      case STRING:
        return new Literal(LiteralType.STRING, decodeString(token));
      case NUMBER:
        BigDecimal value = new BigDecimal(token.text);
        return new Literal(
            LiteralType.NUMBER, value, token.text.startsWith("-") && value.signum() == 0);
      case TRUE:
        return new Literal(LiteralType.BOOLEAN, true);
      case FALSE:
        return new Literal(LiteralType.BOOLEAN, false);
      case NULL:
        return new Literal(LiteralType.NULL, null);
      default:
        throw error(token, "expected literal");
    }
  }

  private LiteralArray parseLiteralArray() {
    List<Literal> values = new ArrayList<>();
    LiteralType elementType = null;
    do {
      if (values.size() >= RestrictedRegoLimits.MAX_ARRAY_ELEMENTS) {
        throw error(
            "array literal must not exceed "
                + RestrictedRegoLimits.MAX_ARRAY_ELEMENTS
                + " elements");
      }
      if (!isLiteral(current.type)) {
        throw error("expected a literal array element");
      }
      Literal literal = parseLiteral();
      if (elementType == null) {
        elementType = literal.literalType();
      }
      values.add(literal);
    } while (match(TokenType.COMMA));
    expect(TokenType.RBRACKET, "expected ] after array literal");
    return new LiteralArray(elementType, values);
  }

  private boolean match(TokenType type) {
    if (!check(type)) {
      return false;
    }
    advance();
    return true;
  }

  private boolean check(TokenType type) {
    return current.type == type;
  }

  private Token expect(TokenType type, String message) {
    if (!check(type)) {
      throw error(message);
    }
    return advance();
  }

  private Token advance() {
    Token previous = current;
    current = lexer.nextToken();
    return previous;
  }

  private IllegalArgumentException error(String message) {
    return error(current, message);
  }

  private static IllegalArgumentException error(Token token, String message) {
    return new IllegalArgumentException(
        String.format(
            "Invalid restricted-rego-v1 program at line %s, column %s: %s",
            token.line, token.column, message));
  }

  private static boolean isComparisonOperator(TokenType type) {
    return type == TokenType.EQ
        || type == TokenType.NEQ
        || type == TokenType.LT
        || type == TokenType.LTE
        || type == TokenType.GT
        || type == TokenType.GTE
        || type == TokenType.IN;
  }

  private static boolean isLiteral(TokenType type) {
    return type == TokenType.STRING
        || type == TokenType.NUMBER
        || type == TokenType.TRUE
        || type == TokenType.FALSE
        || type == TokenType.NULL;
  }

  private static String decodeString(Token token) {
    StringBuilder result = new StringBuilder();
    for (int index = 1; index < token.text.length() - 1; index++) {
      char current = token.text.charAt(index);
      if (current != '\\') {
        result.append(current);
        continue;
      }

      char escaped = token.text.charAt(++index);
      switch (escaped) {
        case '"':
        case '\\':
        case '/':
          result.append(escaped);
          break;
        case 'b':
          result.append('\b');
          break;
        case 'f':
          result.append('\f');
          break;
        case 'n':
          result.append('\n');
          break;
        case 'r':
          result.append('\r');
          break;
        case 't':
          result.append('\t');
          break;
        case 'u':
          int codeUnit = 0;
          for (int digitIndex = index + 1; digitIndex < index + 5; digitIndex++) {
            char digit = token.text.charAt(digitIndex);
            if (!RestrictedRegoLexer.isAsciiHexDigit(digit)) {
              throw error(token, "invalid Unicode escape in string");
            }
            codeUnit = codeUnit * 16 + RestrictedRegoLexer.asciiHexValue(digit);
          }
          result.append((char) codeUnit);
          index += 4;
          break;
        default:
          throw error(token, "unsupported string escape: \\" + escaped);
      }
    }
    String decoded = result.toString();
    if (decoded.getBytes(StandardCharsets.UTF_8).length > RestrictedRegoLimits.MAX_STRING_BYTES) {
      throw error(
          token,
          "decoded string literal must not exceed "
              + RestrictedRegoLimits.MAX_STRING_BYTES
              + " UTF-8 bytes");
    }
    return decoded;
  }
}
