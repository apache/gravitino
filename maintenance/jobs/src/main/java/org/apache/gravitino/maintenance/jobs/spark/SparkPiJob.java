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
package org.apache.gravitino.maintenance.jobs.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Self-contained Spark Pi program for the built-in SparkPi job template.
 *
 * <p>This avoids depending on the Spark examples jar so the built-in template can run with only the
 * Gravitino-provided jobs artifact on the classpath.
 */
public class SparkPiJob implements BuiltInJob {

  private static final String NAME = JobTemplateProvider.BUILTIN_NAME_PREFIX + "sparkpi";
  // Bump VERSION whenever SparkPi template behavior changes (name/executable/class/args/configs).
  private static final String VERSION = "v1";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Built-in SparkPi job template")
        .withExecutable(resolveExecutable(SparkPiJob.class))
        .withClassName(SparkPiJob.class.getName())
        .withArguments(Collections.singletonList("{{slices}}"))
        .withConfigs(buildSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  public static void main(String[] args) {
    int slices = 2;
    if (args.length > 0) {
      try {
        slices = Integer.parseInt(args[0]);
      } catch (NumberFormatException e) {
        System.err.println("Invalid number of slices provided. Using default value of 2.");
      }
    }

    int samples = Math.max(slices, 1) * 100000;

    SparkSession spark =
        SparkSession.builder()
            .appName("Gravitino Built-in SparkPi")
            // Rely on external cluster/master configuration
            .getOrCreate();

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    JavaRDD<Integer> rdd =
        jsc.parallelize(IntStream.range(0, samples).boxed().collect(Collectors.toList()), slices);
    long count =
        rdd.filter(
                i -> {
                  double x = Math.random() * 2 - 1;
                  double y = Math.random() * 2 - 1;
                  return x * x + y * y <= 1;
                })
            .count();

    double pi = 4.0 * count / samples;
    System.out.printf("Pi is roughly %.5f%n", pi);

    spark.stop();
  }

  private Map<String, String> buildSparkConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.master", "{{spark_master}}");
    configs.put("spark.executor.instances", "{{spark_executor_instances}}");
    configs.put("spark.executor.cores", "{{spark_executor_cores}}");
    configs.put("spark.executor.memory", "{{spark_executor_memory}}");
    configs.put("spark.driver.memory", "{{spark_driver_memory}}");
    return Collections.unmodifiableMap(configs);
  }
}
