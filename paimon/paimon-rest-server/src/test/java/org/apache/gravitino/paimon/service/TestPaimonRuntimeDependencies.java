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
package org.apache.gravitino.paimon.service;

import com.sun.net.httpserver.HttpServer;
import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.gravitino.paimon.RESTService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Verifies provider discovery using production dependencies, without Spark's test classpath. */
public class TestPaimonRuntimeDependencies {

  @Test
  void testServerStartupAndOssSigningWithOnlyProductionDependencies(@TempDir Path directory)
      throws Exception {
    List<URL> urls = new ArrayList<>();
    for (String path : System.getProperty("paimon.runtime-classpath").split(File.pathSeparator)) {
      urls.add(new File(path).toURI().toURL());
    }
    urls.add(RESTService.class.getProtectionDomain().getCodeSource().getLocation());
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader =
        new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> signerClass = loader.loadClass("com.aliyun.credentials.utils.ParameterHelper");
      String signature =
          (String)
              signerClass
                  .getMethod("signString", String.class, String.class)
                  .invoke(signerClass.getConstructor().newInstance(), "payload", "secret");
      Assertions.assertFalse(signature.isEmpty());
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      Class<?> serviceClass = loader.loadClass(RESTService.class.getName());
      Object service = serviceClass.getConstructor().newInstance();
      serviceClass
          .getMethod("serviceInit", Map.class, boolean.class)
          .invoke(
              service,
              Map.of(
                  "host",
                  "127.0.0.1",
                  "httpPort",
                  String.valueOf(port),
                  "catalog-backend",
                  "filesystem",
                  "warehouse",
                  directory.toUri().toString(),
                  "token",
                  "runtime-test"),
              false);
      try {
        serviceClass.getMethod("serviceStart").invoke(service);
        HttpResponse<String> response =
            HttpClient.newHttpClient()
                .send(
                    HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + "/paimon/health/ready"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode(), response.body());
      } finally {
        serviceClass.getMethod("serviceStop").invoke(service);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Test
  void testHiveS3AndPostgresqlAreAvailableInProductionClasspath() throws Exception {
    List<URL> urls = new ArrayList<>();
    for (String path : System.getProperty("paimon.runtime-classpath").split(File.pathSeparator)) {
      urls.add(new File(path).toURI().toURL());
    }
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader =
        new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> factories = loader.loadClass("org.apache.paimon.factories.FactoryUtil");
      Class<?> catalogFactory = loader.loadClass("org.apache.paimon.catalog.CatalogFactory");
      Object hive =
          factories
              .getMethod("discoverFactory", ClassLoader.class, Class.class, String.class)
              .invoke(null, loader, catalogFactory, "hive");
      Assertions.assertEquals("hive", catalogFactory.getMethod("identifier").invoke(hive));
      Class<?> ioLoader = loader.loadClass("org.apache.paimon.fs.FileIOLoader");
      List<String> schemes = new ArrayList<>();
      for (Object provider : ServiceLoader.load(ioLoader, loader)) {
        schemes.add((String) ioLoader.getMethod("getScheme").invoke(provider));
      }
      Assertions.assertTrue(schemes.contains("s3"), schemes.toString());
      Class<?> optionsClass = loader.loadClass("org.apache.paimon.options.Options");
      Object options =
          optionsClass
              .getMethod("fromMap", Map.class)
              .invoke(
                  null,
                  Map.of(
                      "s3.access-key",
                      "test-key",
                      "s3.secret-key",
                      "test-secret",
                      "s3.region",
                      "us-east-1",
                      "s3.endpoint",
                      "http://127.0.0.1:1",
                      "s3.path.style.access",
                      "true"));
      Class<?> contextClass = loader.loadClass("org.apache.paimon.catalog.CatalogContext");
      Object context = contextClass.getMethod("create", optionsClass).invoke(null, options);
      Class<?> pathClass = loader.loadClass("org.apache.paimon.fs.Path");
      Object path =
          pathClass.getConstructor(String.class).newInstance("s3://test-bucket/warehouse");
      Class<?> fileIOClass = loader.loadClass("org.apache.paimon.fs.FileIO");
      // Construct and configure the S3 IO, without connecting to an external storage service.
      try (Closeable io =
          (Closeable)
              fileIOClass.getMethod("get", pathClass, contextClass).invoke(null, path, context)) {
        Assertions.assertTrue((Boolean) fileIOClass.getMethod("isObjectStore").invoke(io));
      }
      Assertions.assertTrue(schemes.contains("oss"), schemes.toString());
      HttpServer storage = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      storage.createContext(
          "/",
          exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
          });
      storage.start();
      try {
        Object ossOptions =
            optionsClass
                .getMethod("fromMap", Map.class)
                .invoke(
                    null,
                    Map.of(
                        "fs.oss.accessKeyId",
                        "test-key",
                        "fs.oss.accessKeySecret",
                        "test-secret",
                        "fs.oss.endpoint",
                        "http://127.0.0.1:" + storage.getAddress().getPort()));
        Object ossContext = contextClass.getMethod("create", optionsClass).invoke(null, ossOptions);
        Object ossPath =
            pathClass.getConstructor(String.class).newInstance("oss://test-bucket/warehouse");
        try (Closeable io =
            (Closeable)
                fileIOClass
                    .getMethod("get", pathClass, contextClass)
                    .invoke(null, ossPath, ossContext)) {
          Assertions.assertTrue((Boolean) fileIOClass.getMethod("isObjectStore").invoke(io));
        }
      } finally {
        storage.stop(0);
      }
      Class<?> driver = loader.loadClass("org.postgresql.Driver");
      Assertions.assertNotNull(driver.getConstructor().newInstance());
      Class<?> jwt = loader.loadClass("io.jsonwebtoken.Jwts");
      Assertions.assertNotNull(jwt.getMethod("parserBuilder").invoke(null));
      Assertions.assertNotNull(loader.loadClass("com.nimbusds.jwt.proc.DefaultJWTProcessor"));
      for (String resource :
          List.of(
              "org/eclipse/jetty/util/thread/QueuedThreadPool.class",
              "javax/servlet/Servlet.class",
              "org/apache/hadoop/conf/Configuration.class",
              "com/github/luben/zstd/Zstd.class")) {
        List<URL> definitions = Collections.list(loader.getResources(resource));
        Assertions.assertEquals(1, definitions.size(), definitions::toString);
      }
      Class<?> zstd = loader.loadClass("com.github.luben.zstd.Zstd");
      String source = zstd.getProtectionDomain().getCodeSource().getLocation().getPath();
      Assertions.assertTrue(
          source.endsWith("/zstd-jni-" + System.getProperty("paimon.zstd-jni.version") + ".jar"),
          source);
      byte[] payload = "Paimon REST catalog".getBytes(StandardCharsets.UTF_8);
      byte[] compressed = (byte[]) zstd.getMethod("compress", byte[].class).invoke(null, payload);
      byte[] decoded =
          (byte[])
              zstd.getMethod("decompress", byte[].class, int.class)
                  .invoke(null, compressed, payload.length);
      Assertions.assertArrayEquals(payload, decoded);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
