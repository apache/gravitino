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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import com.alibaba.qlexpress4.Express4Runner;
import com.alibaba.qlexpress4.InitOptions;
import com.alibaba.qlexpress4.QLOptions;
import com.alibaba.qlexpress4.exception.QLException;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QLExpressionEvaluator implements ExpressionEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(QLExpressionEvaluator.class);

  private static final Express4Runner RUNNER = new Express4Runner(InitOptions.DEFAULT_OPTIONS);

  // Cache compiled regex patterns for hyphen-to-underscore replacement, keyed by the set of
  // hyphenated context keys. Avoids expensive Pattern.compile on every evaluation call.
  private final Map<Set<String>, HyphenReplacementRule> replacementRuleCache =
      new ConcurrentHashMap<>();

  @Override
  public long evaluateLong(String expression, Map<String, Object> context) {
    return toLong(evaluate(expression, context));
  }

  @Override
  public boolean evaluateBool(String expression, Map<String, Object> context) {
    return (boolean) evaluate(expression, context);
  }

  @Override
  public Optional<Boolean> tryToEvaluateBool(String expression, Map<String, Object> context) {
    return tryToEvaluate(expression, context).map(o -> (Boolean) o);
  }

  private Object evaluate(String expression, Map<String, Object> context) {
    Preconditions.checkArgument(StringUtils.isNotBlank(expression), "expression is blank");
    Preconditions.checkArgument(context != null, "context is null");
    String formattedExpression = formatExpression(expression, context);
    return RUNNER
        .execute(formattedExpression, formatContextKey(context), QLOptions.DEFAULT_OPTIONS)
        .getResult();
  }

  private Optional<Object> tryToEvaluate(String expression, Map<String, Object> context) {
    try {
      Object result = evaluate(expression, context);
      return Optional.of(result);
    } catch (QLException e) {
      LOG.warn("Failed to evaluate expression '{}': {}", expression, e.getMessage());
      return Optional.empty();
    }
  }

  private Map<String, Object> formatContextKey(Map<String, Object> context) {
    return context.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> normalizeIdentifier(entry.getKey()), entry -> entry.getValue()));
  }

  private String formatExpression(String expression, Map<String, Object> context) {
    HyphenReplacementRule rule = getReplacementRule(context);
    if (rule == null) {
      return expression;
    }
    Matcher matcher = rule.pattern.matcher(expression);
    StringBuffer buffer = new StringBuffer();
    while (matcher.find()) {
      String matched = matcher.group(1);
      matcher.appendReplacement(
          buffer, Matcher.quoteReplacement(rule.replacements.getOrDefault(matched, matched)));
    }
    matcher.appendTail(buffer);
    return buffer.toString();
  }

  /** Return a cached replacement rule for the hyphenated keys in the context, or null if none. */
  private HyphenReplacementRule getReplacementRule(Map<String, Object> context) {
    Set<String> hyphenatedKeys =
        context.keySet().stream()
            .filter(key -> !key.equals(normalizeIdentifier(key)))
            .collect(Collectors.toSet());
    if (hyphenatedKeys.isEmpty()) {
      return null;
    }
    return replacementRuleCache.computeIfAbsent(
        hyphenatedKeys,
        keys -> {
          Map<String, String> replacements =
              keys.stream()
                  .collect(
                      Collectors.toMap(
                          key -> key, this::normalizeIdentifier, (left, right) -> left));
          String alternation =
              replacements.keySet().stream().map(Pattern::quote).collect(Collectors.joining("|"));
          Pattern pattern =
              Pattern.compile("(?<![A-Za-z0-9_])(" + alternation + ")(?![A-Za-z0-9_])");
          return new HyphenReplacementRule(pattern, replacements);
        });
  }

  /** Pre-compiled pattern and replacement map for rewriting hyphenated identifiers. */
  private static class HyphenReplacementRule {
    final Pattern pattern;
    final Map<String, String> replacements;

    HyphenReplacementRule(Pattern pattern, Map<String, String> replacements) {
      this.pattern = pattern;
      this.replacements = replacements;
    }
  }

  private String normalizeIdentifier(String name) {
    return name.replace("-", "_");
  }

  private Long toLong(Object obj) {
    if (obj instanceof Long) {
      return (Long) obj;
    }

    if (obj instanceof Integer) {
      return ((Integer) obj).longValue();
    }

    if (obj instanceof BigDecimal) {
      return ((BigDecimal) obj).longValue();
    }

    if (obj instanceof Number) {
      if (obj instanceof Double || obj instanceof Float) {
        return Math.round(((Number) obj).doubleValue());
      }
      return ((Number) obj).longValue();
    }

    throw new IllegalArgumentException("Object cannot be converted to Long");
  }
}
