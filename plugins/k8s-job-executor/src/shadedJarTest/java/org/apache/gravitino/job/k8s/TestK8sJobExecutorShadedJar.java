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
package org.apache.gravitino.job.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.connector.job.JobContext;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.job.JobExecutorFactory;
import org.apache.gravitino.job.SparkJobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the shaded jar of the k8s job executor the way the Gravitino server loads it: the jar is on
 * the classpath of the server, next to the server's own Jackson, and neither the classes nor the
 * dependencies of the module are. The tests refer to the job executor by name only.
 */
public class TestK8sJobExecutorShadedJar {

  private static final String SHADED_PACKAGE = "org.apache.gravitino.k8s.shaded.";

  private static final String SPARK_APPLICATIONS_PATH =
      "/apis/spark.apache.org/v1/namespaces/default/sparkapplications";

  private static File shadedJar;

  @BeforeAll
  public static void setUp() throws IOException {
    String path = System.getProperty("gravitino.k8s.shadedJar");
    Assertions.assertNotNull(path, "Run the tests with the shadedJarTest Gradle task");
    shadedJar = new File(path).getCanonicalFile();
    Assertions.assertTrue(shadedJar.isFile(), shadedJar + " doesn't exist");
  }

  @Test
  public void testJacksonIsRelocated() throws Exception {
    Assertions.assertEquals(
        shadedJar, getJarOf(Class.forName(JobExecutorFactory.K8S_JOB_EXECUTOR_CLASS_NAME)));
    // fabric8 uses the Jackson of the shaded jar, the server keeps its own.
    Assertions.assertEquals(
        shadedJar, getJarOf(Class.forName(SHADED_PACKAGE + ObjectMapper.class.getName())));
    Assertions.assertNotEquals(shadedJar, getJarOf(ObjectMapper.class));

    try (JarFile jar = new JarFile(shadedJar)) {
      Set<String> notRelocated = new TreeSet<>();
      for (JarEntry entry : Collections.list(jar.entries())) {
        String name = entry.getName();
        if (name.startsWith("com/fasterxml/")
            || name.startsWith("org/yaml/")
            || name.startsWith("org/snakeyaml/")) {
          notRelocated.add(name);
        }
      }
      Assertions.assertTrue(notRelocated.isEmpty(), "Not relocated: " + notRelocated);
    }
  }

  @Test
  public void testNoClassOfServerClasspathIsBundled() throws IOException {
    // A class that is both in the shaded jar and in another jar of the server is loaded from
    // whichever comes first on the classpath, so the shaded jar must not bundle any. The classpath
    // here is the runtime classpath of :server, the libs directory of the distribution also gets
    // the jars of the other plugins (idp-basic, tms and its dependencies), which this doesn't
    // cover.
    Set<String> serverClasses = new TreeSet<>();
    for (String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
      File file = new File(path).getCanonicalFile();
      if (file.isFile() && file.getName().endsWith(".jar") && !file.equals(shadedJar)) {
        serverClasses.addAll(listClasses(file));
      }
    }
    Assertions.assertFalse(serverClasses.isEmpty());

    Set<String> bundled = new TreeSet<>(listClasses(shadedJar));
    Assertions.assertFalse(bundled.isEmpty());
    bundled.retainAll(serverClasses);
    Assertions.assertTrue(
        bundled.isEmpty(), "Classes also on the classpath of the server: " + bundled);
  }

  @Test
  public void testCreateJobExecutorAndSubmitJob(@TempDir Path jobDir) throws Exception {
    List<String> requests = Collections.synchronizedList(new ArrayList<>());
    List<String> bodies = Collections.synchronizedList(new ArrayList<>());
    // A minimal API server, which creates whatever is posted to it.
    HttpServer apiServer =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    apiServer.createContext(
        "/",
        exchange -> {
          byte[] body = ByteStreams.toByteArray(exchange.getRequestBody());
          requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
          boolean created =
              "POST".equals(exchange.getRequestMethod())
                  && SPARK_APPLICATIONS_PATH.equals(exchange.getRequestURI().getPath());
          if (created) {
            bodies.add(new String(body, StandardCharsets.UTF_8));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, body.length);
          } else {
            body = new byte[0];
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
          }
          try (OutputStream response = exchange.getResponseBody()) {
            response.write(body);
          }
        });
    apiServer.start();

    try {
      Config config = new Config(false) {};
      config.set(Configs.JOB_EXECUTOR, JobExecutorFactory.K8S_JOB_EXECUTOR_NAME);
      config.loadFromMap(
          ImmutableMap.of(
              "gravitino.jobExecutor.k8s.masterUrl",
              "http://127.0.0.1:" + apiServer.getAddress().getPort(),
              "gravitino.jobExecutor.k8s.namespace",
              "default",
              "gravitino.jobExecutor.k8s.spark.image",
              "gravitino-spark:3.5.9",
              "gravitino.jobExecutor.k8s.spark.sparkVersion",
              "3.5.9",
              "gravitino.jobExecutor.k8s.spark.conf.spark.executor.instances",
              "2"),
          key -> true);
      SparkJobTemplate template =
          SparkJobTemplate.builder()
              .withName("builtin-sparkpi")
              .withExecutable("/gravitino/auxlib/gravitino-jobs.jar")
              .withClassName("org.apache.gravitino.maintenance.jobs.spark.SparkPiJob")
              .withConfigs(ImmutableMap.of("spark.master", "local[*]"))
              .build();

      // The factory finds the built-in k8s job executor by its class name.
      try (JobExecutor jobExecutor = JobExecutorFactory.create(config)) {
        Assertions.assertEquals(
            JobExecutorFactory.K8S_JOB_EXECUTOR_CLASS_NAME, jobExecutor.getClass().getName());

        String jobExecutionId =
            jobExecutor.submitJob(new JobContext(1L, "metalake", jobDir.toFile()), template);
        Assertions.assertEquals("default/sparkapp/default/gravitino-job-1", jobExecutionId);
      }
    } finally {
      apiServer.stop(0);
    }

    Assertions.assertEquals(1, bodies.size(), "Requests: " + requests);
    // Written by the relocated Jackson of the shaded jar, read by the Jackson of the server.
    JsonNode sparkApplication = new ObjectMapper().readTree(bodies.get(0));
    Assertions.assertEquals("SparkApplication", sparkApplication.at("/kind").asText());
    Assertions.assertEquals("gravitino-job-1", sparkApplication.at("/metadata/name").asText());
    Assertions.assertEquals(
        "local:///opt/gravitino/jobs/gravitino-jobs.jar",
        sparkApplication.at("/spec/jars").asText());
    JsonNode sparkConf = sparkApplication.at("/spec/sparkConf");
    Assertions.assertEquals("2", sparkConf.get("spark.executor.instances").asText());
    Assertions.assertFalse(sparkConf.has("spark.master"));
  }

  private static File getJarOf(Class<?> clazz) throws IOException, URISyntaxException {
    return new File(clazz.getProtectionDomain().getCodeSource().getLocation().toURI())
        .getCanonicalFile();
  }

  private static Set<String> listClasses(File jarFile) throws IOException {
    Set<String> classes = new TreeSet<>();
    try (JarFile jar = new JarFile(jarFile)) {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        // Every jar may have its own module descriptor and multi-release classes.
        if (name.endsWith(".class")
            && !name.startsWith("META-INF/")
            && !name.endsWith("module-info.class")) {
          classes.add(name);
        }
      }
    }
    return classes;
  }
}
