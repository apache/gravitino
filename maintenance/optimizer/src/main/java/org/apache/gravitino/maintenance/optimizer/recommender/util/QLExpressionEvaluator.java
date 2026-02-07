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
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class QLExpressionEvaluator implements ExpressionEvaluator {
  private static final Express4Runner RUNNER = new Express4Runner(InitOptions.DEFAULT_OPTIONS);

  @Override
  public long evaluateLong(String expression, Map<String, Object> context) {
    return toLong(evaluate(expression, context));
  }

  @Override
  public boolean evaluateBool(String expression, Map<String, Object> context) {
    return (boolean) evaluate(expression, context);
  }

  private Object evaluate(String expression, Map<String, Object> context) {
    Preconditions.checkArgument(StringUtils.isNotBlank(expression), "expression is blank");
    Preconditions.checkArgument(context != null, "context is null");
    String formattedExpression = formatExpression(expression, context);
    return RUNNER
        .execute(formattedExpression, formatContextKey(context), QLOptions.DEFAULT_OPTIONS)
        .getResult();
  }

  private Map<String, Object> formatContextKey(Map<String, Object> context) {
    return context.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> normalizeIdentifier(entry.getKey()), entry -> entry.getValue()));
  }

  private String formatExpression(String expression, Map<String, Object> context) {
    Map<String, String> replacements =
        context.keySet().stream()
            .collect(
                Collectors.toMap(key -> key, this::normalizeIdentifier, (left, right) -> left));
    replacements.entrySet().removeIf(entry -> entry.getKey().equals(entry.getValue()));
    if (replacements.isEmpty()) {
      return expression;
    }

    String alternation =
        replacements.keySet().stream().map(Pattern::quote).collect(Collectors.joining("|"));
    Pattern pattern = Pattern.compile("(?<![A-Za-z0-9_])(" + alternation + ")(?![A-Za-z0-9_])");
    Matcher matcher = pattern.matcher(expression);
    StringBuffer buffer = new StringBuffer();
    while (matcher.find()) {
      String matched = matcher.group(1);
      matcher.appendReplacement(
          buffer, Matcher.quoteReplacement(replacements.getOrDefault(matched, matched)));
    }
    matcher.appendTail(buffer);
    return buffer.toString();
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
