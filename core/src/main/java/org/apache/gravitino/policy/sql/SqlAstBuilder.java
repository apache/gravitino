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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlBaseVisitor;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.AndExpressionContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.CaseExpressionContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.DataTypeContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.DefinitionContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.ExpressionContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.FunctionCallContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.IdentifierContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.LiteralContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.NotExpressionContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.OrExpressionContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.ParameterContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.PredicateContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.ScalarContext;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser.WhenClauseContext;

/** Converts an ANTLR parse tree into operation-specific immutable SQL nodes. */
final class SqlAstBuilder extends RestrictedSqlBaseVisitor<SqlExpression> {
  private final Map<String, SqlType> parameters = new LinkedHashMap<>();
  private int nodes;
  private int listElements;
  private int activeOperations;

  SqlDefinition build(DefinitionContext context, boolean mask) {
    for (ParameterContext parameter : context.parameter()) {
      String name = identifier(parameter.identifier());
      SqlType type = type(parameter.dataType());
      require(parameters.putIfAbsent(name, type) == null, parameter, "duplicate parameter " + name);
      require(
          parameters.size() <= RestrictedSqlLimits.MAX_PARAMETERS,
          parameter,
          "too many parameters");
    }
    SqlType result = type(context.dataType());
    if (mask) {
      require(parameters.size() == 1, context, "column mask requires exactly one parameter");
      require(
          result.equals(parameters.values().iterator().next()),
          context,
          "mask parameter and result types must match");
    } else {
      require(result.equals(SqlType.BOOLEAN), context, "row filter must return BOOLEAN");
    }
    SqlExpression body = visit(context.expression());
    new SqlTypeValidator().validate(body, result);
    return new SqlDefinition(parameters, result, body);
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visit(ParseTree tree) {
    // Grammar precedence and parentheses add parse-tree layers, but no AST operations.
    // Unwrap them iteratively so legal nested parentheses do not consume the visitor stack.
    ParseTree current = tree;
    while (true) {
      if (current instanceof ExpressionContext) {
        current = ((ExpressionContext) current).orExpression();
      } else if (current instanceof OrExpressionContext
          && ((OrExpressionContext) current).andExpression().size() == 1) {
        current = ((OrExpressionContext) current).andExpression(0);
      } else if (current instanceof AndExpressionContext
          && ((AndExpressionContext) current).notExpression().size() == 1) {
        current = ((AndExpressionContext) current).notExpression(0);
      } else if (current instanceof NotExpressionContext
          && ((NotExpressionContext) current).NOT().isEmpty()) {
        current = ((NotExpressionContext) current).predicate();
      } else if (current instanceof PredicateContext && current.getChildCount() == 1) {
        current = ((PredicateContext) current).scalar(0);
      } else if (current instanceof ScalarContext
          && ((ScalarContext) current).expression() != null) {
        current = ((ScalarContext) current).expression();
      } else {
        break;
      }
    }
    boolean operation =
        current instanceof OrExpressionContext
            || current instanceof AndExpressionContext
            || current instanceof NotExpressionContext
            || current instanceof PredicateContext
            || current instanceof FunctionCallContext
            || current instanceof CaseExpressionContext;
    if (!operation) {
      return super.visit(current);
    }
    require(
        ++activeOperations <= RestrictedSqlLimits.MAX_DEPTH,
        (ParserRuleContext) current,
        "operation depth must not exceed " + RestrictedSqlLimits.MAX_DEPTH);
    try {
      return super.visit(current);
    } finally {
      activeOperations--;
    }
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitExpression(ExpressionContext context) {
    return visit(context.orExpression());
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitOrExpression(OrExpressionContext context) {
    List<SqlExpression> children = new ArrayList<>();
    context.andExpression().forEach(child -> children.add(visit(child)));
    return children.size() == 1
        ? children.get(0)
        : node(new LogicalExpression(LogicalOperator.OR, children), context);
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitAndExpression(AndExpressionContext context) {
    List<SqlExpression> children = new ArrayList<>();
    context.notExpression().forEach(child -> children.add(visit(child)));
    return children.size() == 1
        ? children.get(0)
        : node(new LogicalExpression(LogicalOperator.AND, children), context);
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitNotExpression(NotExpressionContext context) {
    require(
        context.NOT().size() <= RestrictedSqlLimits.MAX_DEPTH,
        context,
        "operation depth must not exceed " + RestrictedSqlLimits.MAX_DEPTH);
    SqlExpression result = visit(context.predicate());
    for (int i = 0; i < context.NOT().size(); i++) {
      result = node(new Not(result), context);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitPredicate(PredicateContext context) {
    SqlExpression left = visit(context.scalar(0));
    if (context.comparisonOperator() != null) {
      return node(
          new Comparison(
              ComparisonOperator.fromToken(context.comparisonOperator().getText()),
              left,
              visit(context.scalar(1))),
          context);
    }
    if (context.IS() != null) {
      return node(new IsNull(left, context.NOT() != null), context);
    }
    SqlExpression result;
    if (context.IN() != null) {
      List<Literal> values = new ArrayList<>();
      for (LiteralContext literal : context.literal()) {
        require(
            ++listElements <= RestrictedSqlLimits.MAX_IN_ELEMENTS,
            context,
            "IN lists must not exceed " + RestrictedSqlLimits.MAX_IN_ELEMENTS + " total elements");
        values.add((Literal) visit(literal));
      }
      result = node(new In(left, values), context);
    } else if (context.BETWEEN() != null) {
      result = node(new Between(left, visit(context.scalar(1)), visit(context.scalar(2))), context);
    } else if (context.LIKE() != null) {
      String pattern = quoted(context.STRING().getText());
      require(
          pattern.length() > 1
              && pattern.endsWith("%")
              && pattern.indexOf('%') == pattern.length() - 1
              && pattern.indexOf('_') < 0,
          context,
          "LIKE requires a non-empty fixed prefix followed by one %");
      Literal literal = node(new Literal(LiteralType.STRING, pattern, SqlType.STRING), context);
      result = node(new Like(left, literal), context);
    } else {
      return left;
    }
    return context.NOT() == null ? result : node(new Not(result), context);
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitScalar(ScalarContext context) {
    if (context.identifier() != null) {
      String name = identifier(context.identifier());
      require(parameters.containsKey(name), context, "undeclared parameter " + name);
      return node(new InputReference(name, parameters.get(name)), context);
    }
    if (context.literal() != null) {
      return visit(context.literal());
    }
    if (context.functionCall() != null) {
      return visit(context.functionCall());
    }
    if (context.caseExpression() != null) {
      return visit(context.caseExpression());
    }
    return visit(context.expression());
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitFunctionCall(FunctionCallContext context) {
    require(
        context.identifier().QUOTED_IDENTIFIER() == null,
        context,
        "quoted function names are unsupported");
    SqlFunction function = SqlFunction.fromName(context.identifier().getText());
    List<SqlExpression> arguments = new ArrayList<>();
    context.expression().forEach(argument -> arguments.add(visit(argument)));
    return node(new FunctionCall(function, arguments), context);
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitCaseExpression(CaseExpressionContext context) {
    SqlExpression selector =
        context.getChild(1) instanceof ExpressionContext ? visit(context.expression(0)) : null;
    List<CaseBranch> branches = new ArrayList<>();
    for (WhenClauseContext branch : context.whenClause()) {
      branches.add(new CaseBranch(visit(branch.expression(0)), visit(branch.expression(1))));
    }
    SqlExpression fallback =
        context.ELSE() == null ? null : visit(context.expression(context.expression().size() - 1));
    return node(new CaseExpression(selector, branches, fallback), context);
  }

  /** {@inheritDoc} */
  @Override
  public SqlExpression visitLiteral(LiteralContext context) {
    if (context.NUMBER() != null) {
      return node(new Literal(LiteralType.NUMBER, context.getText(), null), context);
    }
    if (context.NULL() != null) {
      return node(new Literal(LiteralType.NULL, "", null), context);
    }
    if (context.TRUE() != null || context.FALSE() != null) {
      return node(
          new Literal(
              LiteralType.BOOLEAN, context.getText().toUpperCase(Locale.ROOT), SqlType.BOOLEAN),
          context);
    }
    String value = quoted(context.STRING().getText());
    if (context.getStart().getType() == RestrictedSqlParser.STRING) {
      return node(new Literal(LiteralType.STRING, value, SqlType.STRING), context);
    }
    String name = context.getStart().getText().toUpperCase(Locale.ROOT);
    return node(
        new Literal(
            LiteralType.TEMPORAL,
            value,
            new SqlType(name, name.equals("DATE") ? 0 : 6, 0, context.WITH() != null)),
        context);
  }

  private SqlType type(DataTypeContext context) {
    String name = context.getStart().getText().toUpperCase(Locale.ROOT);
    int precision = 0;
    int scale = 0;
    if (name.equals("DECIMAL")) {
      precision = unsignedInteger(context.NUMBER(0).getText(), context);
      scale = unsignedInteger(context.NUMBER(1).getText(), context);
      require(
          precision >= 1 && precision <= 38 && scale <= precision,
          context,
          "invalid decimal precision or scale");
    } else if (name.equals("TIME") || name.equals("TIMESTAMP")) {
      precision = 6;
      require(
          context.NUMBER().isEmpty() || unsignedInteger(context.NUMBER(0).getText(), context) == 6,
          context,
          "temporal precision must be 6");
    }
    return new SqlType(name, precision, scale, context.WITH() != null);
  }

  private static int unsignedInteger(String source, ParserRuleContext context) {
    require(source.matches("[0-9]{1,2}"), context, "expected unsigned integer");
    return Integer.parseInt(source);
  }

  private static String identifier(IdentifierContext context) {
    String name =
        context.QUOTED_IDENTIFIER() == null
            ? context.getText().toUpperCase(Locale.ROOT)
            : quoted(context.getText());
    require(!name.isEmpty(), context, "identifier cannot be empty");
    return name;
  }

  private static String quoted(String text) {
    String quote = text.substring(0, 1);
    return text.substring(1, text.length() - 1).replace(quote + quote, quote);
  }

  private <T extends SqlExpression> T node(T expression, ParserRuleContext context) {
    require(
        ++nodes <= RestrictedSqlLimits.MAX_NODES,
        context,
        "expression must not exceed " + RestrictedSqlLimits.MAX_NODES + " nodes");
    require(
        expression.depth() <= RestrictedSqlLimits.MAX_DEPTH,
        context,
        "operation depth must not exceed " + RestrictedSqlLimits.MAX_DEPTH);
    return expression;
  }

  private static void require(boolean valid, ParserRuleContext context, String message) {
    if (!valid) {
      throw RestrictedSqlSourceParser.error(context.getStart(), message);
    }
  }
}
