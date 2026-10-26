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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.sql.RestrictedSqlLexer.Token;

/** Recursive descent over the complete restricted SQL definition. */
final class RestrictedSqlParser {
  private static final String RESERVED =
      "RETURNS BOOLEAN INTEGER LONG FLOAT DOUBLE STRING BINARY DATE DECIMAL TIME TIMESTAMP WITH WITHOUT ZONE "
          + "OR AND NOT IS NULL IN BETWEEN LIKE CASE WHEN THEN ELSE END TRUE FALSE";

  private final RestrictedSqlLexer lexer;
  private final Map<String, SqlType> parameters = new LinkedHashMap<>();
  private Token token;
  private int nodes;
  private int listElements;
  private int parentheses;
  private int operations;

  RestrictedSqlParser(String source) {
    lexer = new RestrictedSqlLexer(source);
    token = lexer.next();
  }

  SqlDefinition parse(boolean mask) {
    expect("(");
    if (!token.is(")")) {
      do {
        String name = identifier();
        SqlType type = type();
        require(parameters.putIfAbsent(name, type) == null, "duplicate parameter " + name);
        require(parameters.size() <= 256, "too many parameters");
      } while (accept(","));
    }
    expect(")");
    expect("RETURNS");
    SqlType result = type();
    expect("->");
    SqlExpression body = expression();
    expect("EOF");
    require(body.depth() <= 8, "operation depth must not exceed 8");
    if (mask) {
      require(parameters.size() == 1, "column mask requires exactly one parameter");
      require(
          result.equals(parameters.values().iterator().next()),
          "mask parameter and result types must match");
    } else {
      require(result.equals(SqlType.BOOLEAN), "row filter must return BOOLEAN");
    }
    new SqlTypeValidator().validate(body, result);
    return new SqlDefinition(parameters, result, body);
  }

  private SqlType type() {
    require(token.kind.equals("WORD"), "expected type");
    String name = take().text;
    require(
        name.matches("BOOLEAN|INTEGER|LONG|FLOAT|DOUBLE|STRING|BINARY|DATE|DECIMAL|TIME|TIMESTAMP"),
        "unsupported type " + name);
    int precision = 0;
    int scale = 0;
    boolean zone = false;
    if (name.equals("DECIMAL")) {
      expect("(");
      precision = unsignedInteger();
      expect(",");
      scale = unsignedInteger();
      expect(")");
      require(
          precision >= 1 && precision <= 38 && scale <= precision,
          "invalid decimal precision or scale");
    } else if (name.equals("TIME") || name.equals("TIMESTAMP")) {
      precision = 6;
      if (accept("(")) {
        require(unsignedInteger() == 6, "temporal precision must be 6");
        expect(")");
      }
      if (name.equals("TIMESTAMP") && (token.is("WITH") || token.is("WITHOUT"))) {
        zone = take().is("WITH");
        expect("TIME");
        expect("ZONE");
      }
    }
    return new SqlType(name, precision, scale, zone);
  }

  private int unsignedInteger() {
    require(
        token.kind.equals("NUMBER") && token.text.matches("[0-9]{1,2}"),
        "expected unsigned integer");
    return Integer.parseInt(take().text);
  }

  private SqlExpression expression() {
    return logical("OR");
  }

  private SqlExpression logical(String operator) {
    List<SqlExpression> children = new ArrayList<>();
    do {
      children.add(operator.equals("OR") ? logical("AND") : not());
    } while (accept(operator));
    return children.size() == 1 ? children.get(0) : node(operator, "", null, children);
  }

  private SqlExpression not() {
    if (accept("NOT")) {
      enterOperation();
      SqlExpression child = not();
      operations--;
      return node("NOT", child);
    }
    return predicate();
  }

  private SqlExpression predicate() {
    SqlExpression left = scalar();
    if (token.kind.matches("=|<>|!=|<|<=|>|>=")) {
      String operator = take().text;
      return node(operator.equals("!=") ? "<>" : operator, left, scalar());
    }
    if (accept("IS")) {
      boolean negated = accept("NOT");
      expect("NULL");
      return node(negated ? "IS NOT NULL" : "IS NULL", left);
    }
    boolean negated = accept("NOT");
    SqlExpression result;
    if (accept("IN")) {
      expect("(");
      List<SqlExpression> values = new ArrayList<>();
      values.add(left);
      do {
        require(++listElements <= 256, "IN lists must not exceed 256 total elements");
        values.add(literal());
      } while (accept(","));
      expect(")");
      result = node("IN", "", null, values);
    } else if (accept("BETWEEN")) {
      SqlExpression lower = scalar();
      expect("AND");
      result = node("BETWEEN", left, lower, scalar());
    } else if (accept("LIKE")) {
      require(token.kind.equals("STRING"), "LIKE requires a literal prefix");
      String pattern = token.text;
      require(
          pattern.length() > 1
              && pattern.endsWith("%")
              && pattern.indexOf('%') == pattern.length() - 1
              && pattern.indexOf('_') < 0,
          "LIKE requires a non-empty fixed prefix followed by one %");
      result = node("LIKE", left, literal());
    } else {
      require(!negated, "expected IN, BETWEEN or LIKE after NOT");
      return left;
    }
    return negated ? node("NOT", result) : result;
  }

  private SqlExpression scalar() {
    if (accept("(")) {
      require(++parentheses <= 256, "parenthesis nesting must not exceed 256");
      SqlExpression value = expression();
      expect(")");
      parentheses--;
      return value;
    }
    if (accept("CASE")) {
      enterOperation();
      List<SqlExpression> children = new ArrayList<>();
      boolean searched = token.is("WHEN");
      if (!searched) {
        children.add(expression());
      }
      expect("WHEN");
      do {
        children.add(expression());
        expect("THEN");
        children.add(expression());
      } while (accept("WHEN"));
      children.add(accept("ELSE") ? expression() : node("NULL", "", null, Collections.emptyList()));
      expect("END");
      operations--;
      return node(searched ? "CASE" : "SIMPLE CASE", "", null, children);
    }
    if (token.kind.equals("QUOTED") || token.kind.equals("WORD") && !reserved(token.text)) {
      boolean quoted = token.kind.equals("QUOTED");
      String name = identifier();
      if (accept("(")) {
        require(!quoted, "quoted function names are unsupported");
        enterOperation();
        List<SqlExpression> args = new ArrayList<>();
        if (!token.is(")")) {
          do {
            args.add(expression());
          } while (accept(","));
        }
        expect(")");
        operations--;
        return node("CALL", name, null, args);
      }
      require(parameters.containsKey(name), "undeclared parameter " + name);
      return node("INPUT", name, parameters.get(name), Collections.emptyList());
    }
    return literal();
  }

  private SqlExpression literal() {
    Token value = take();
    if (value.kind.equals("STRING")) {
      return node("STRING", value.text, SqlType.STRING, Collections.emptyList());
    }
    if (value.kind.equals("NUMBER")) {
      return node("NUMBER", value.text, null, Collections.emptyList());
    }
    if (value.is("NULL")) {
      return node("NULL", "", null, Collections.emptyList());
    }
    if (value.is("TRUE") || value.is("FALSE")) {
      return node("BOOLEAN", value.text, SqlType.BOOLEAN, Collections.emptyList());
    }
    if (value.is("DATE") || value.is("TIME") || value.is("TIMESTAMP")) {
      boolean zone = false;
      if (value.is("TIMESTAMP") && accept("WITH")) {
        expect("TIME");
        expect("ZONE");
        zone = true;
      }
      require(token.kind.equals("STRING"), "expected temporal string literal");
      return node(
          "TEMPORAL",
          take().text,
          new SqlType(value.text, value.is("DATE") ? 0 : 6, 0, zone),
          Collections.emptyList());
    }
    throw lexer.error("expected scalar literal, found " + value.text);
  }

  private String identifier() {
    require(
        token.kind.equals("QUOTED") || token.kind.equals("WORD") && !reserved(token.text),
        "expected identifier");
    String name = take().text;
    require(!name.isEmpty(), "identifier cannot be empty");
    return name;
  }

  private static boolean reserved(String name) {
    return Arrays.asList(RESERVED.split(" ")).contains(name);
  }

  private SqlExpression node(String operator, SqlExpression... children) {
    return node(operator, "", null, Arrays.asList(children));
  }

  private SqlExpression node(
      String operator, String value, @Nullable SqlType type, List<SqlExpression> children) {
    require(++nodes <= 256, "expression must not exceed 256 nodes");
    SqlExpression result = new SqlExpression(operator, value, type, children);
    require(result.depth() <= 8, "operation depth must not exceed 8");
    return result;
  }

  private void enterOperation() {
    require(++operations <= 8, "operation depth must not exceed 8");
  }

  private Token take() {
    Token current = token;
    token = lexer.next();
    return current;
  }

  private boolean accept(String text) {
    if (!token.is(text)) {
      return false;
    }
    take();
    return true;
  }

  private void expect(String text) {
    require(accept(text), "expected " + text + ", found " + token.text);
  }

  private void require(boolean valid, String message) {
    if (!valid) {
      throw lexer.error(message);
    }
  }
}
