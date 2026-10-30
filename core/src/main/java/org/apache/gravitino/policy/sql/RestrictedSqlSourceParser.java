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
import java.util.List;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlLexer;
import org.apache.gravitino.policy.sql.antlr.RestrictedSqlParser;

/** Runs the generated ANTLR parser with fail-fast diagnostics and bounded recursive work. */
final class RestrictedSqlSourceParser {
  private RestrictedSqlSourceParser() {}

  static SqlDefinition parse(String source, boolean mask) {
    validateUnicode(source);
    RestrictedSqlLexer lexer = new RestrictedSqlLexer(CharStreams.fromString(source));
    lexer.removeErrorListeners();
    lexer.addErrorListener(new SyntaxErrors());
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    tokens.fill();
    validateTokens(tokens.getTokens(), source);
    RestrictedSqlParser parser = new RestrictedSqlParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SyntaxErrors());
    parser.addParseListener(new RecursionLimit());
    return new SqlAstBuilder().build(parser.definition(), mask);
  }

  static IllegalArgumentException error(Token token, String message) {
    return new IllegalArgumentException(
        "Invalid restricted-sql-v1 definition at line "
            + token.getLine()
            + ", column "
            + (token.getCharPositionInLine() + 1)
            + ": "
            + message);
  }

  private static void validateUnicode(String source) {
    for (int i = 0; i < source.length(); i++) {
      char c = source.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (++i >= source.length() || !Character.isLowSurrogate(source.charAt(i))) {
          throw new IllegalArgumentException(
              "Invalid restricted-sql-v1 source: isolated surrogate");
        }
      } else if (Character.isLowSurrogate(c) || c == 0) {
        throw new IllegalArgumentException(
            "Invalid restricted-sql-v1 source: invalid Unicode character");
      }
    }
  }

  private static void validateTokens(List<Token> tokens, String source) {
    int parentheses = 0;
    for (Token token : tokens) {
      if (token.getType() == RestrictedSqlLexer.LPAREN
          && ++parentheses > RestrictedSqlLimits.MAX_PARENTHESES) {
        throw error(
            token, "parenthesis nesting must not exceed " + RestrictedSqlLimits.MAX_PARENTHESES);
      }
      if (token.getType() == RestrictedSqlLexer.RPAREN) {
        parentheses--;
      }
      if (token.getType() == RestrictedSqlLexer.NUMBER) {
        if (token.getText().length() > RestrictedSqlLimits.MAX_NUMBER_BYTES) {
          throw error(
              token, "numeric literal exceeds " + RestrictedSqlLimits.MAX_NUMBER_BYTES + " bytes");
        }
        int next = token.getStopIndex() + 1;
        // ANTLR offsets count code points. Numeric tokens contain only ASCII, but their prefix may
        // not.
        int sourceIndex = source.offsetByCodePoints(0, next);
        if (sourceIndex < source.length()
            && (Character.isLetterOrDigit(source.charAt(sourceIndex))
                || source.charAt(sourceIndex) == '_')) {
          throw error(token, "numeric literal must end at a token boundary");
        }
      }
      if (token.getType() == RestrictedSqlLexer.STRING
          || token.getType() == RestrictedSqlLexer.QUOTED_IDENTIFIER) {
        String quote = token.getType() == RestrictedSqlLexer.STRING ? "'" : "\"";
        String decoded =
            token
                .getText()
                .substring(1, token.getText().length() - 1)
                .replace(quote + quote, quote);
        if (decoded.getBytes(StandardCharsets.UTF_8).length
            > RestrictedSqlLimits.MAX_STRING_BYTES) {
          throw error(
              token,
              "quoted token exceeds " + RestrictedSqlLimits.MAX_STRING_BYTES + " UTF-8 bytes");
        }
      }
    }
  }

  private static final class SyntaxErrors extends BaseErrorListener {
    /** {@inheritDoc} */
    @Override
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int column,
        String message,
        RecognitionException exception) {
      throw new IllegalArgumentException(
          "Invalid restricted-sql-v1 definition at line "
              + line
              + ", column "
              + (column + 1)
              + ": "
              + message);
    }
  }

  private static final class RecursionLimit implements ParseTreeListener {
    private int operations;
    /** {@inheritDoc} */
    @Override
    public void enterEveryRule(ParserRuleContext context) {
      if (recursive(context) && ++operations > RestrictedSqlLimits.MAX_DEPTH) {
        throw error(
            context.getStart(), "operation depth must not exceed " + RestrictedSqlLimits.MAX_DEPTH);
      }
    }
    /** {@inheritDoc} */
    @Override
    public void exitEveryRule(ParserRuleContext context) {
      if (recursive(context)) {
        operations--;
      }
    }
    /** {@inheritDoc} */
    @Override
    public void visitTerminal(TerminalNode node) {}
    /** {@inheritDoc} */
    @Override
    public void visitErrorNode(ErrorNode node) {}

    private static boolean recursive(ParserRuleContext context) {
      return context.getRuleIndex() == RestrictedSqlParser.RULE_caseExpression
          || context.getRuleIndex() == RestrictedSqlParser.RULE_functionCall;
    }
  }
}
