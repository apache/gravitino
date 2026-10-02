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
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.FilterBranch;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.MaskBranch;
import org.apache.gravitino.policy.expression.RestrictedRegoProgram.RowFilter;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionBaseVisitor;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionLexer;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.AndExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ArrayLiteralContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ColumnReferenceContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ComparisonExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.FilterElseBranchContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.FilterRuleContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.GroupMembershipContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.LiteralContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.MaskActionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.MaskElseBranchContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.MaskRuleContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.NotExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.OrExpressionContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.PrimaryContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.ProgramContext;
import org.apache.gravitino.policy.expression.antlr.RestrictedRegoExpressionParser.SessionUserReferenceContext;

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

    SyntaxErrorListener errorListener = new SyntaxErrorListener();
    RestrictedRegoExpressionLexer lexer =
        new RestrictedRegoExpressionLexer(CharStreams.fromString(source));
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);

    RestrictedRegoExpressionParser parser =
        new RestrictedRegoExpressionParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);

    ProgramContext context = parser.program();
    if (errorListener.errorMessage != null) {
      throw new IllegalArgumentException(errorListener.errorMessage);
    }

    ExpressionVisitor visitor = new ExpressionVisitor();
    RestrictedRegoProgram program =
        context.filterRule() == null
            ? buildColumnMask(context.maskRule(), visitor)
            : buildRowFilter(context.filterRule(), visitor);
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

  private static RowFilter buildRowFilter(FilterRuleContext context, ExpressionVisitor visitor) {
    if (context.expression().size() == 1) {
      return new RowFilter(new ArrayList<>(), visitor.visit(context.expression(0)));
    }

    List<FilterBranch> branches = new ArrayList<>();
    branches.add(
        new FilterBranch(
            visitor.visit(context.expression(0)), visitor.visit(context.expression(1))));
    for (FilterElseBranchContext branchContext : context.filterElseBranch()) {
      branches.add(
          new FilterBranch(
              visitor.visit(branchContext.expression(0)),
              visitor.visit(branchContext.expression(1))));
    }
    return new RowFilter(branches, visitor.visit(context.expression(2)));
  }

  private static ColumnMask buildColumnMask(MaskRuleContext context, ExpressionVisitor visitor) {
    if (context.maskAction().size() == 1) {
      return new ColumnMask(new ArrayList<>(), parseMaskAction(context.maskAction(0)));
    }

    List<MaskBranch> branches = new ArrayList<>();
    branches.add(
        new MaskBranch(
            parseMaskAction(context.maskAction(0)), visitor.visit(context.expression())));
    for (MaskElseBranchContext branchContext : context.maskElseBranch()) {
      branches.add(
          new MaskBranch(
              parseMaskAction(branchContext.maskAction()),
              visitor.visit(branchContext.expression())));
    }
    return new ColumnMask(branches, parseMaskAction(context.maskAction(1)));
  }

  private static MaskAction parseMaskAction(MaskActionContext context) {
    return MaskAction.fromValue(decodeString(context.STRING().getText()));
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
                "Invalid restricted-rego-v1 program at line %s, column %s: %s",
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
