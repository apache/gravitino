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

package org.apache.gravitino.job.local;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.job.SparkJobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSparkProcessBuilder {

  @Test
  public void testGenerateSparkSubmitCommand() {
    String sparkSubmitPath = "/path/to/spark-submit";

    SparkJobTemplate template1 =
        SparkJobTemplate.builder()
            .withName("template1")
            .withExecutable("/path/to/spark-demo.jar")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .withClassName("com.example.MainClass")
            .build();

    List<String> command1 =
        SparkProcessBuilder.generateSparkSubmitCommand(sparkSubmitPath, template1);

    Assertions.assertEquals(sparkSubmitPath, command1.get(0));
    Assertions.assertTrue(command1.contains("--class"));

    int idx = command1.indexOf("--class");
    Assertions.assertEquals("com.example.MainClass", command1.get(idx + 1));

    Assertions.assertTrue(command1.contains("/path/to/spark-demo.jar"));

    Assertions.assertTrue(command1.contains("arg1"));
    Assertions.assertTrue(command1.contains("arg2"));

    SparkJobTemplate template2 =
        SparkJobTemplate.builder()
            .withName("template2")
            .withExecutable("/path/to/spark-demo.py")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .build();

    List<String> command2 =
        SparkProcessBuilder.generateSparkSubmitCommand(sparkSubmitPath, template2);

    Assertions.assertEquals(sparkSubmitPath, command2.get(0));
    Assertions.assertFalse(command2.contains("--class"));

    Assertions.assertTrue(command2.contains("/path/to/spark-demo.py"));
    Assertions.assertTrue(command2.contains("arg1"));
    Assertions.assertTrue(command2.contains("arg2"));

    SparkJobTemplate template3 =
        SparkJobTemplate.builder()
            .withName("template3")
            .withExecutable("/path/to/spark-demo.jar")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .withJars(Lists.newArrayList("/path/to/lib1.jar", "/path/to/lib2.jar"))
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withClassName("com.example.MainClass")
            .build();

    List<String> command3 =
        SparkProcessBuilder.generateSparkSubmitCommand(sparkSubmitPath, template3);

    Assertions.assertEquals(sparkSubmitPath, command3.get(0));
    Assertions.assertTrue(command3.contains("--class"));

    int idx3 = command3.indexOf("--class");
    Assertions.assertEquals("com.example.MainClass", command3.get(idx3 + 1));

    Assertions.assertTrue(command3.contains("--jars"));
    int idxJars = command3.indexOf("--jars");
    Assertions.assertEquals("/path/to/lib1.jar,/path/to/lib2.jar", command3.get(idxJars + 1));

    Assertions.assertTrue(command3.contains("--files"));
    int idxFiles = command3.indexOf("--files");
    Assertions.assertEquals("/path/to/file1.txt,/path/to/file2.txt", command3.get(idxFiles + 1));

    Assertions.assertTrue(command3.contains("--archives"));
    int idxArchives = command3.indexOf("--archives");
    Assertions.assertEquals(
        "/path/to/archive1.zip,/path/to/archive2.zip", command3.get(idxArchives + 1));

    Assertions.assertTrue(command3.contains("/path/to/spark-demo.jar"));
    Assertions.assertTrue(command3.contains("arg1"));
    Assertions.assertTrue(command3.contains("arg2"));

    SparkJobTemplate template4 =
        SparkJobTemplate.builder()
            .withName("template4")
            .withExecutable("/path/to/spark-demo.jar")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .withClassName("com.example.MainClass")
            .withConfigs(
                ImmutableMap.of(
                    "spark.executor.memory", "2g",
                    "spark.executor.cores", "2"))
            .build();

    List<String> command4 =
        SparkProcessBuilder.generateSparkSubmitCommand(sparkSubmitPath, template4);

    Assertions.assertEquals(sparkSubmitPath, command4.get(0));
    Assertions.assertTrue(command4.contains("--class"));

    int idx4 = command4.indexOf("--class");
    Assertions.assertEquals("com.example.MainClass", command4.get(idx4 + 1));

    Assertions.assertTrue(command4.contains("--conf"));
    int idxConf1 = command4.indexOf("--conf");
    Assertions.assertEquals("spark.executor.memory=2g", command4.get(idxConf1 + 1));

    int idxConf2 = command4.lastIndexOf("--conf");
    Assertions.assertEquals("spark.executor.cores=2", command4.get(idxConf2 + 1));

    Assertions.assertTrue(command4.contains("/path/to/spark-demo.jar"));
    Assertions.assertTrue(command4.contains("arg1"));
    Assertions.assertTrue(command4.contains("arg2"));
  }
}
