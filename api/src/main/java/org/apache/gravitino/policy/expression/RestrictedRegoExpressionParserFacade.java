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
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.expression.CanonicalExpression.GroupMembership;
import org.apache.gravitino.policy.expression.CanonicalExpression.Literal;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralArray;
import org.apache.gravitino.policy.expression.CanonicalExpression.LiteralType;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operation;
import org.apache.gravitino.policy.expression.CanonicalExpression.Operator;
import org.apache.gravitino.policy.expression.CanonicalExpression.SessionUser;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionBaseVisitor;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionLexer;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.AndExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ArrayLiteralContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ColumnReferenceContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ComparisonExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.GroupMembershipContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.LiteralContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.NotExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.OrExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.PrimaryContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.SessionUserReferenceContext;

/** Parses and validates expressions in the {@code restricted-rego-v1} source dialect. */
public final class RestrictedRegoExpressionParserFacade {

  private static final int MAX_SOURCE_BYTES = 16 * 1024;
  private static final int MAX_SOURCE_DEPTH = 8;
  private static final int MAX_AST_NODES = 256;
  private static final int MAX_STRING_BYTES = 4 * 1024;
  private static final int MAX_ARRAY_ELEMENTS = 256;

  private RestrictedRegoExpressionParserFacade() {}

  /**
   * Parses and validates one complete row-filter expression.
   *
   * @param expression expression in {@code restricted-rego-v1}
   * @return validated unresolved expression tree
   * @throws IllegalArgumentException if syntax or semantics are invalid
   */
  public static CanonicalExpression parse(String expression) {
    return parseInternal(expression, false);
  }

  /**
   * Parses and validates a context-only column-mask condition.
   *
   * <p>The condition supports the same dialect as a row filter but rejects every {@code col(...)}
   * reference.
   *
   * @param expression condition in {@code restricted-rego-v1}
   * @return validated unresolved context expression tree
   * @throws IllegalArgumentException if syntax or semantics are invalid or data-dependent
   */
  public static CanonicalExpression parseContextCondition(String expression) {
    return parseInternal(expression, true);
  }

  private static CanonicalExpression parseInternal(String expression, boolean contextOnly) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(expression), "restricted-rego-v1 expression cannot be blank");
    Preconditions.checkArgument(
        expression.getBytes(StandardCharsets.UTF_8).length <= MAX_SOURCE_BYTES,
        "restricted-rego-v1 source must not exceed %s UTF-8 bytes",
        MAX_SOURCE_BYTES);

    SyntaxErrorListener errorListener = new SyntaxErrorListener();
    RestrictedRegoExpressionLexer lexer =
        new RestrictedRegoExpressionLexer(CharStreams.fromString(expression));
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);

    RestrictedRegoExpressionParser parser =
        new RestrictedRegoExpressionParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);

    ExpressionContext context = parser.expression();
    if (errorListener.errorMessage != null) {
      throw new IllegalArgumentException(errorListener.errorMessage);
    }

    CanonicalExpression result = new ExpressionVisitor().visitExpression(context);
    Preconditions.checkArgument(
        ExpressionValidation.isPredicate(result),
        "restricted-rego-v1 expression root must be a boolean predicate");
    result.validate();
    Preconditions.checkArgument(
        result.depth() <= MAX_SOURCE_DEPTH,
        "restricted-rego-v1 source depth must not exceed %s",
        MAX_SOURCE_DEPTH);
    Preconditions.checkArgument(
        countNodes(result) <= MAX_AST_NODES,
        "restricted-rego-v1 AST must not exceed %s nodes",
        MAX_AST_NODES);
    if (contextOnly) {
      Preconditions.checkArgument(
          isContextOnly(result), "column-mask condition cannot contain col(...)");
    }
    return result;
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

  private static boolean isContextOnly(CanonicalExpression expression) {
    if (expression instanceof CanonicalExpression.Column) {
      return false;
    }
    if (!(expression instanceof Operation)) {
      return true;
    }

    Operation operation = (Operation) expression;
    if (operation.left() != null && !isContextOnly(operation.left())) {
      return false;
    }
    if (operation.right() != null && !isContextOnly(operation.right())) {
      return false;
    }
    if (operation.operand() != null && !isContextOnly(operation.operand())) {
      return false;
    }
    if (operation.operands() != null) {
      for (CanonicalExpression child : operation.operands()) {
        if (!isContextOnly(child)) {
          return false;
        }
      }
    }
    return true;
  }

  private static final class SyntaxErrorListener extends BaseErrorListener {
    private String errorMessage;

    @Override
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int charPositionInLine,
        String message,
        RecognitionException exception) {
      if (errorMessage == null) {
        errorMessage =
            String.format(
                "Invalid restricted-rego-v1 expression at line %s, column %s: %s",
                line, charPositionInLine, message);
      }
    }
  }

  private static final class ExpressionVisitor
      extends RestrictedRegoExpressionBaseVisitor<CanonicalExpression> {

    @Override
    public CanonicalExpression visitExpression(ExpressionContext context) {
      return visit(context.orExpression());
    }

    @Override
    public CanonicalExpression visitOrExpression(OrExpressionContext context) {
      CanonicalExpression result = visit(context.andExpression(0));
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(result), "or operands must be boolean predicates");
      for (int index = 1; index < context.andExpression().size(); index++) {
        CanonicalExpression operand = visit(context.andExpression(index));
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(operand), "or operands must be boolean predicates");
        result = new Operation(Operator.OR, null, null, null, Arrays.asList(result, operand));
      }
      return result;
    }

    @Override
    public CanonicalExpression visitAndExpression(AndExpressionContext context) {
      CanonicalExpression result = visit(context.notExpression(0));
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(result), "and operands must be boolean predicates");
      for (int index = 1; index < context.notExpression().size(); index++) {
        CanonicalExpression operand = visit(context.notExpression(index));
        Preconditions.checkArgument(
            ExpressionValidation.isPredicate(operand), "and operands must be boolean predicates");
        result = new Operation(Operator.AND, null, null, null, Arrays.asList(result, operand));
      }
      return result;
    }

    @Override
    public CanonicalExpression visitNotExpression(NotExpressionContext context) {
      if (context.NOT() == null) {
        return visit(context.comparisonExpression());
      }

      CanonicalExpression operand = visit(context.notExpression());
      Preconditions.checkArgument(
          ExpressionValidation.isPredicate(operand), "not operand must be a boolean predicate");
      return new Operation(Operator.NOT, null, null, operand, null);
    }

    @Override
    public CanonicalExpression visitComparisonExpression(ComparisonExpressionContext context) {
      CanonicalExpression left = visit(context.primary(0));
      if (context.comparisonOperator() == null) {
        return left;
      }

      CanonicalExpression right = visit(context.primary(1));
      Operator operator = Operator.fromValue(context.comparisonOperator().getText());
      return new Operation(operator, left, right, null, null);
    }

    @Override
    public CanonicalExpression visitPrimary(PrimaryContext context) {
      if (context.columnReference() != null) {
        return visit(context.columnReference());
      }
      if (context.sessionUserReference() != null) {
        return visit(context.sessionUserReference());
      }
      if (context.groupMembership() != null) {
        return visit(context.groupMembership());
      }
      if (context.literal() != null) {
        return visit(context.literal());
      }
      if (context.arrayLiteral() != null) {
        return visit(context.arrayLiteral());
      }
      return visit(context.orExpression());
    }

    @Override
    public CanonicalExpression visitColumnReference(ColumnReferenceContext context) {
      return new CanonicalExpression.Column(decodeString(context.STRING().getText()));
    }

    @Override
    public CanonicalExpression visitSessionUserReference(SessionUserReferenceContext context) {
      return new SessionUser();
    }

    @Override
    public CanonicalExpression visitGroupMembership(GroupMembershipContext context) {
      return new GroupMembership(decodeString(context.STRING().getText()));
    }

    @Override
    public CanonicalExpression visitLiteral(LiteralContext context) {
      if (context.STRING() != null) {
        return new Literal(LiteralType.STRING, decodeString(context.STRING().getText()));
      }
      if (context.NUMBER() != null) {
        String token = context.NUMBER().getText();
        BigDecimal value = new BigDecimal(token);
        return new Literal(LiteralType.NUMBER, value, token.startsWith("-") && value.signum() == 0);
      }
      if (context.TRUE() != null) {
        return new Literal(LiteralType.BOOLEAN, true);
      }
      if (context.FALSE() != null) {
        return new Literal(LiteralType.BOOLEAN, false);
      }
      return new Literal(LiteralType.NULL, null);
    }

    @Override
    public CanonicalExpression visitArrayLiteral(ArrayLiteralContext context) {
      Preconditions.checkArgument(
          context.literal().size() <= MAX_ARRAY_ELEMENTS,
          "array literal must not exceed %s elements",
          MAX_ARRAY_ELEMENTS);
      List<Literal> values = new ArrayList<>();
      LiteralType elementType = null;
      for (LiteralContext child : context.literal()) {
        Literal literal = (Literal) visit(child);
        Preconditions.checkArgument(
            literal.literalType() != LiteralType.NULL, "array literals cannot contain null");
        Preconditions.checkArgument(
            elementType == null || elementType == literal.literalType(),
            "array literals must have one homogeneous source type");
        elementType = literal.literalType();
        values.add(literal);
      }
      return new LiteralArray(elementType, values);
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
