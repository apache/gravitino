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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the published runtime artifact from an ordinary Gradle consumer's perspective: the
 * standard Java variants must expose the shaded jar without republishing the dependencies bundled
 * into it. See https://github.com/apache/gravitino/issues/13171
 */
class TestRuntimePublicationConsumer {
  @TempDir Path consumerProject;

  @Test
  void testGradleConsumerResolvesShadedJarWithoutBundledDependencies() throws IOException {
    String coordinate = System.getProperty("runtimeCoordinate");
    String expectedJar = System.getProperty("expectedRuntimeJar");
    String repo = System.getProperty("consumerRepo");
    write("settings.gradle", "rootProject.name = 'runtime-consumer'\n");
    write(
        "build.gradle",
        """
        plugins { id 'java' }
        repositories { maven { url = findProperty('repoUrl') } }
        dependencies { implementation '%s' }
        tasks.register('verifyConsumer', JavaExec) {
          dependsOn compileJava
          classpath = sourceSets.main.runtimeClasspath
          mainClass = 'Consumer'
          doFirst {
            def artifacts = configurations.runtimeClasspath.files
            println "RUNTIME ARTIFACTS: " + artifacts.collect { it.name }
            assert artifacts.size() == 1 : "Expected only the shaded jar, got: " + artifacts
            assert artifacts[0].name == '%s' : "Expected the shaded jar, got: " + artifacts[0]
          }
        }
        """
            .formatted(coordinate, expectedJar));
    write(
        "src/main/java/Consumer.java",
        """
        import org.apache.gravitino.client.GravitinoClient;

        public class Consumer {
          public static void main(String[] args) throws Exception {
            // Compile-time reference: fails compilation if apiElements lacks the class.
            Class<GravitinoClient> type = GravitinoClient.class;
            Class<?> loaded = Class.forName(type.getName());
            System.out.println("LOADED: " + loaded.getName());
          }
        }
        """);

    BuildResult result =
        GradleRunner.create()
            .withProjectDir(consumerProject.toFile())
            .withArguments("verifyConsumer", "-PrepoUrl=" + repo, "--stacktrace")
            .build();
    assertTrue(
        result.getOutput().contains("LOADED: org.apache.gravitino.client.GravitinoClient"),
        result.getOutput());
  }

  private void write(String relativePath, String content) throws IOException {
    Path file = consumerProject.resolve(relativePath);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content, StandardCharsets.UTF_8);
  }
}
