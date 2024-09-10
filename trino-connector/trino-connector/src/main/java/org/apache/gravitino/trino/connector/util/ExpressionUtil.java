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
package org.apache.gravitino.trino.connector.util;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_EXPRESSION_ERROR;

import io.trino.spi.TrinoException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;

/** This class is used to convert expression of bucket, sort_by, partition object to string */
public class ExpressionUtil {
  private static final String IDENTIFIER = "[a-zA-Z_][a-zA-Z0-9_]*";
  private static final String FUNCTION_ARG_INT = "(\\d+)";
  private static final String FUNCTION_ARG_IDENTIFIER = "(" + IDENTIFIER + ")";
  private static final Pattern YEAR_FUNCTION_PATTERN =
      Pattern.compile("year\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern MONTH_FUNCTION_PATTERN =
      Pattern.compile("month\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern DAY_FUNCTION_PATTERN =
      Pattern.compile("day\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern HOUR_FUNCTION_PATTERN =
      Pattern.compile("hour\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern BUCKET_FUNCTION_PATTERN =
      Pattern.compile(
          "bucket\\(" + FUNCTION_ARG_IDENTIFIER + ",\\s*" + FUNCTION_ARG_INT + "\\)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern TRUNCATE_FUNCTION_PATTERN =
      Pattern.compile(
          "truncate\\(" + FUNCTION_ARG_IDENTIFIER + ",\\s*" + FUNCTION_ARG_INT + "\\)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern IDENTIFILER_PATTERN =
      Pattern.compile(IDENTIFIER, Pattern.CASE_INSENSITIVE);

  public static List<String> expressionToPartitionFiled(Transform[] transforms) {
    try {
      List<String> partitionFields = new ArrayList<>();
      for (Transform transform : transforms) {
        partitionFields.add(transFormToString(transform));
      }
      return partitionFields;
    } catch (IllegalArgumentException e) {
      throw new TrinoException(
          GRAVITINO_EXPRESSION_ERROR, "Error to handle Transform Expressions : ", e);
    }
  }

  public static Transform[] partitionFiledToExpression(List<String> partitions) {
    try {
      List<Transform> partitionTransForms = new ArrayList<>();
      for (String partition : partitions) {
        parseTransForm(partitionTransForms, partition);
      }
      return partitionTransForms.toArray(new Transform[0]);
    } catch (IllegalArgumentException e) {
      throw new TrinoException(GRAVITINO_EXPRESSION_ERROR, "Error parsing partition field: ", e);
    }
  }

  private static void parseTransForm(List<Transform> transforms, String value) {
    boolean match =
        false
            || tryMatch(
                value,
                IDENTIFILER_PATTERN,
                (m) -> {
                  transforms.add(Transforms.identity(m.group(0)));
                })
            || tryMatch(
                value,
                YEAR_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.year(m.group(1)));
                })
            || tryMatch(
                value,
                MONTH_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.month(m.group(1)));
                })
            || tryMatch(
                value,
                DAY_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.day(m.group(1)));
                })
            || tryMatch(
                value,
                HOUR_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.hour(m.group(1)));
                })
            || tryMatch(
                value,
                BUCKET_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(
                      Transforms.bucket(Integer.parseInt(m.group(2)), new String[] {m.group(1)}));
                })
            || tryMatch(
                value,
                TRUNCATE_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(
                      Transforms.truncate(Integer.parseInt(m.group(2)), new String[] {m.group(1)}));
                });
    if (!match) {
      throw new IllegalArgumentException("Unparsed expression: " + value);
    }
  }

  private static boolean tryMatch(String value, Pattern pattern, MatcherHandler handler) {
    Matcher matcher = pattern.matcher(value);
    if (matcher.matches()) {
      handler.invoke(matcher.toMatchResult());
      return true;
    }
    return false;
  }

  private static String transFormToString(Transform transform) {
    if (transform instanceof Transforms.IdentityTransform) {
      return ((Transforms.IdentityTransform) transform).fieldName()[0];
    } else {
      return functionTransFormToString(transform);
    }
  }

  private static String functionTransFormToString(Transform transform) {
    if (transform.arguments().length == 1) {
      return String.format("%s(%s)", transform.name(), transform.arguments()[0]);
    } else if (transform.arguments().length == 2) {
      return String.format(
          "%s(%s, %s)", transform.name(), transform.arguments()[0], transform.arguments()[1]);
    }
    throw new IllegalArgumentException(
        String.format(
            "Unsupported transform %s with %d parameters: ",
            transform, transform.arguments().length));
  }

  interface MatcherHandler {
    void invoke(MatchResult matchResult);
  }
}
