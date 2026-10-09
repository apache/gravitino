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
package org.apache.gravitino.policy.expression;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.expression.CanonicalExpression.GroupMembership;
import org.apache.gravitino.policy.expression.CanonicalExpression.Literal;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralArray;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralType;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operation;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operator;
import org.apache.gravitino.policy.expression.CanonicalExpression.SessionUser;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.FilterBranch;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.MaskBranch;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.RowFilter;

/** Parses and validates complete programs in the {@code restricted-rego-v1} source dialect. */
public final class RestrictedRegoExpressionParserFacade {

  private static final int MAX_SOURCE_BYTES = 16 * 1024;
  private static final int MAX_SOURCE_DEPTH = 8;
  private static final int MAX_AST_NODES = 256;
  private static final int MAX_STRING_BYTES = 4 * 1024;
  private static final int MAX_ARRAY_ELEMENTS = 256;

  private RestrictedRegoExpressionParserFacade() {}

  /**
   * Parses and validates one complete row-filter or column-mask program.
   *
   * @param source program in {@code restricted-rego-v1}
   * @return validated unresolved program
   * @throws IllegalArgumentException if syntax or semantics are invalid
   */
  public static RestrictedRegoProgram parse(String source) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(source), "restricted-rego-v1 program cannot be blank");
    Preconditions.checkArgument(
        source.getBytes(StandardCharsets.UTF_8).length <= MAX_SOURCE_BYTES,
        "restricted-rego-v1 source must not exceed %s UTF-8 bytes",
        MAX_SOURCE_BYTES);

    RestrictedRegoProgram program = new SourceParser(source).parseProgram();
    validateProgram(program);
    return program;
  }

  /**
   * Parses and validates one complete {@code filter := ...} program.
   *
   * @param source row-filter program in {@code restricted-rego-v1}
   * @return validated unresolved row-filter rule
   * @throws IllegalArgumentException if syntax or semantics are invalid or the rule head is not
   *     {@code filter}
   */
  public static RowFilter parseRowFilter(String source) {
    RestrictedRegoProgram program = parse(source);
    Preconditions.checkArgument(
        program instanceof RowFilter, "row-filter program must declare a filter rule");
    return (RowFilter) program;
  }

  /**
   * Parses and validates one complete {@code mask := action(...)} program.
   *
   * @param source column-mask program in {@code restricted-rego-v1}
   * @return validated unresolved column-mask rule
   * @throws IllegalArgumentException if syntax or semantics are invalid or the rule head is not
   *     {@code mask}
   */
  public static ColumnMask parseColumnMask(String source) {
    RestrictedRegoProgram program = parse(source);
    Preconditions.checkArgument(
        program instanceof ColumnMask, "column-mask program must declare a mask rule");
    return (ColumnMask) program;
  }

  private static void validateProgram(RestrictedRegoProgram program) {
    program.validate();
    List<CanonicalExpression> expressions = expressions(program);
    int nodeCount = 0;
    for (CanonicalExpression expression : expressions) {
      Preconditions.checkArgument(
          expression.depth() <= MAX_SOURCE_DEPTH,
          "restricted-rego-v1 source depth must not exceed %s",
          MAX_SOURCE_DEPTH);
      nodeCount += countNodes(expression);
    }
    Preconditions.checkArgument(
        nodeCount <= MAX_AST_NODES,
        "restricted-rego-v1 AST must not exceed %s nodes",
        MAX_AST_NODES);
  }

  private static List<CanonicalExpression> expressions(RestrictedRegoProgram program) {
    List<CanonicalExpression> expressions = new ArrayList<>();
    if (program instanceof RowFilter) {
      RowFilter filter = (RowFilter) program;
      for (FilterBranch branch : filter.branches()) {
        expressions.add(branch.result());
        expressions.add(branch.condition());
      }
      expressions.add(filter.fallback());
    } else {
      ColumnMask mask = (ColumnMask) program;
      for (MaskBranch branch : mask.branches()) {
        expressions.add(branch.condition());
      }
    }
    return expressions;
  }

  private static int countNodes(CanonicalExpression expression) {
    if (!(expression instanceof Operation)) {
      return 1;
    }

    Operation operation = (Operation) expression;
    int count = 1;
    if (operation.left() != null) {
      count += countNodes(operation.left());
    }
    if (operation.right() != null) {
      count += countNodes(operation.right());
    }
    if (operation.operand() != null) {
      count += countNodes(operation.operand());
    }
    if (operation.operands() != null) {
      for (CanonicalExpression child : operation.operands()) {
        count += countNodes(child);
      }
    }
    return count;
  }

  private static final class SourceParser {
    private static final int MAX_PARENTHESIS_NESTING = 256;

    private final Lexer lexer;
    private Token current;
    private int parenthesisNesting;

    private SourceParser(String source) {
      this.lexer = new Lexer(source);
      this.current = lexer.nextToken();
    }

    private RestrictedRegoProgram parseProgram() {
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
      String action = decodeString(expect(TokenType.STRING, "expected mask action string").text);
      expect(TokenType.RPAREN, "expected ) after mask action");
      return MaskAction.fromValue(action);
    }

    private CanonicalExpression parseExpression() {
      return parseOrExpression();
    }

    private CanonicalExpression parseOrExpression() {
      CanonicalExpression result = parseAndExpression();
      while (match(TokenType.OR)) {
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(result), "or operands must be boolean predicates");
        CanonicalExpression operand = parseAndExpression();
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(operand), "or operands must be boolean predicates");
        result = new Operation(Operator.OR, null, null, null, Arrays.asList(result, operand));
      }
      return result;
    }

    private CanonicalExpression parseAndExpression() {
      CanonicalExpression result = parseNotExpression();
      while (match(TokenType.AND)) {
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(result), "and operands must be boolean predicates");
        CanonicalExpression operand = parseNotExpression();
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(operand), "and operands must be boolean predicates");
        result = new Operation(Operator.AND, null, null, null, Arrays.asList(result, operand));
      }
      return result;
    }

    private CanonicalExpression parseNotExpression() {
      int notCount = 0;
      while (match(TokenType.NOT)) {
        notCount++;
      }

      CanonicalExpression result = parseComparisonExpression();
      for (int index = 0; index < notCount; index++) {
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(result), "not operand must be a boolean predicate");
        result = new Operation(Operator.NOT, null, null, result, null);
      }
      return result;
    }

    private CanonicalExpression parseComparisonExpression() {
      CanonicalExpression left = parsePrimary();
      if (!isComparisonOperator(current.type)) {
        return left;
      }

      Operator operator = Operator.fromValue(advance().text);
      return new Operation(operator, left, parsePrimary(), null, null);
    }

    private CanonicalExpression parsePrimary() {
      if (match(TokenType.COL)) {
        expect(TokenType.LPAREN, "expected ( after col");
        String column = decodeString(expect(TokenType.STRING, "expected column name string").text);
        expect(TokenType.RPAREN, "expected ) after column name");
        return new CanonicalExpression.Column(column);
      }
      if (match(TokenType.SESSION_USER)) {
        expect(TokenType.LPAREN, "expected ( after session_user");
        expect(TokenType.RPAREN, "expected ) after session_user(");
        return new SessionUser();
      }
      if (match(TokenType.IS_GROUP_MEMBER)) {
        expect(TokenType.LPAREN, "expected ( after is_group_member");
        String group = decodeString(expect(TokenType.STRING, "expected group name string").text);
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
        Preconditions.checkArgument(
            parenthesisNesting <= MAX_PARENTHESIS_NESTING,
            "restricted-rego-v1 parenthesis nesting must not exceed %s",
            MAX_PARENTHESIS_NESTING);
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
          return new Literal(LiteralType.STRING, decodeString(token.text));
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
        Preconditions.checkArgument(
            values.size() < MAX_ARRAY_ELEMENTS,
            "array literal must not exceed %s elements",
            MAX_ARRAY_ELEMENTS);
        Preconditions.checkArgument(isLiteral(current.type), "array literal cannot be empty");
        Literal literal = parseLiteral();
        Preconditions.checkArgument(
            literal.literalType() != LiteralType.NULL, "array literals cannot contain null");
        Preconditions.checkArgument(
            elementType == null || elementType == literal.literalType(),
            "array literals must have one homogeneous source type");
        elementType = literal.literalType();
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
  }

  private enum TokenType {
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

  private static final class Token {
    private final TokenType type;
    private final String text;
    private final int line;
    private final int column;

    private Token(TokenType type, String text, int line, int column) {
      this.type = type;
      this.text = text;
      this.line = line;
      this.column = column;
    }
  }

  private static final class Lexer {
    private final String source;
    private int offset;
    private int line = 1;
    private int column;

    private Lexer(String source) {
      this.source = source;
    }

    private Token nextToken() {
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
        return new Token(keywordType(text), text, tokenLine, tokenColumn);
      }
      if (isDigit(current) || (current == '-' && isDigit(peekNext()))) {
        scanNumber();
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
            if (atEnd() || Character.digit(advance(), 16) < 0) {
              throw lexicalError(tokenLine, tokenColumn, "invalid Unicode escape in string");
            }
          }
        } else if ("\"\\/bfnrt".indexOf(escaped) < 0) {
          throw lexicalError(tokenLine, tokenColumn, "unsupported string escape");
        }
      }
      throw lexicalError(tokenLine, tokenColumn, "unterminated string");
    }

    private void scanNumber() {
      if (peek() == '-') {
        advance();
      }
      if (peek() == '0') {
        advance();
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
        column = 0;
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

    private static TokenType keywordType(String text) {
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
          throw new IllegalArgumentException(
              "Unsupported identifier in restricted-rego-v1: " + text);
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

    private static IllegalArgumentException lexicalError(int line, int column, String message) {
      return new IllegalArgumentException(
          String.format(
              "Invalid restricted-rego-v1 program at line %s, column %s: %s",
              line, column, message));
    }
  }

  private static String decodeString(String token) {
    StringBuilder result = new StringBuilder();
    for (int index = 1; index < token.length() - 1; index++) {
      char current = token.charAt(index);
      if (current != '\\') {
        result.append(current);
        continue;
      }

      char escaped = token.charAt(++index);
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
          result.append((char) Integer.parseInt(token.substring(index + 1, index + 5), 16));
          index += 4;
          break;
        default:
          throw new IllegalArgumentException("Unsupported string escape: \\" + escaped);
      }
    }
    String decoded = result.toString();
    Preconditions.checkArgument(
        decoded.getBytes(StandardCharsets.UTF_8).length <= MAX_STRING_BYTES,
        "decoded string literal must not exceed %s UTF-8 bytes",
        MAX_STRING_BYTES);
    return decoded;
  }
}
