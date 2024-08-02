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
package org.apache.gravitino.spark.connector.integration.test.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.HiveResult;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConverters;

public class SQLQueryTestHelper {
  private static final String notIncludedMsg = "[not included in comparison]";
  private static final String clsName = SQLQueryTestHelper.class.getCanonicalName();
  private static final String emptySchema = new StructType().catalogString();

  private static String replaceNotIncludedMsg(String line) {
    line =
        line.replaceAll("#\\d+", "#x")
            .replaceAll("plan_id=\\d+", "plan_id=x")
            .replaceAll(
                "Location.*" + clsName + "/", "Location " + notIncludedMsg + "/{warehouse_dir}/")
            .replaceAll("file:[^\\s,]*" + clsName, "file:" + notIncludedMsg + "/{warehouse_dir}")
            .replaceAll("Created By.*", "Created By " + notIncludedMsg)
            .replaceAll("Created Time.*", "Created Time " + notIncludedMsg)
            .replaceAll("Last Access.*", "Last Access " + notIncludedMsg)
            .replaceAll("Partition Statistics\t\\d+", "Partition Statistics\t" + notIncludedMsg)
            .replaceAll("\\s+$", "")
            .replaceAll("\\*\\(\\d+\\) ", "*");
    return line;
  }

  public static Pair<String, List<String>> getNormalizedResult(SparkSession session, String sql) {
    Dataset<Row> df = session.sql(sql);
    String schema = df.schema().catalogString();
    List<String> answer =
        SQLExecution.withNewExecutionId(
            df.queryExecution(),
            Option.apply(""),
            () ->
                JavaConverters.seqAsJavaList(
                        HiveResult.hiveResultString(df.queryExecution().executedPlan()))
                    .stream()
                    .map(s -> replaceNotIncludedMsg(s))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList()));

    Collections.sort(answer);

    return Pair.of(schema, answer);
  }

  // Different Spark version may produce different exceptions, so here just produce
  // [SPARK_EXCEPTION]
  public static Pair<String, List<String>> handleExceptions(
      Supplier<Pair<String, List<String>>> result) {
    try {
      return result.get();
    } catch (Throwable e) {
      return Pair.of(emptySchema, Arrays.asList("[SPARK_EXCEPTION]"));
    }
  }
}
