/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.bundles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Checks the content of the AWS bundle, which is built on hadoop-aws 3.4 and the single modules of
 * the AWS SDK v2. The bundle must not carry the AWS SDK v1 or the 650 MB AWS SDK v2 bundle, must
 * keep the Netty copy on the version of the build, and must keep the S3A connector working.
 */
class TestAwsBundleContent {

  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));
  private static final String NETTY_VERSION = System.getProperty("nettyVersion");

  @Test
  void testNoAwsSdkV1() throws Exception {
    try (JarFile jar = new JarFile(BUNDLE)) {
      assertFalse(
          jar.stream().anyMatch(entry -> entry.getName().startsWith("com/amazonaws/")),
          "The AWS SDK v1 must not be bundled");
      assertFalse(
          jar.stream()
              .anyMatch(entry -> entry.getName().startsWith("META-INF/maven/com.amazonaws/")),
          "The AWS SDK v1 must not be bundled");
    }
  }

  @Test
  void testNoAwsSdkBundleArtifact() throws Exception {
    try (JarFile jar = new JarFile(BUNDLE)) {
      assertFalse(
          jar.stream()
              .anyMatch(
                  entry ->
                      entry
                          .getName()
                          .equals("META-INF/maven/software.amazon.awssdk/bundle/pom.properties")),
          "software.amazon.awssdk:bundle must not be bundled");
      assertFalse(
          jar.stream()
              .anyMatch(
                  entry ->
                      entry
                          .getName()
                          .startsWith("software/amazon/awssdk/thirdparty/org/apache/http/")),
          "The Apache HTTP client shaded by the AWS SDK bundle must not be bundled");
    }
  }

  @Test
  void testNettyVersion() throws Exception {
    assertNotNull(NETTY_VERSION, "nettyVersion");
    List<String> versions = new ArrayList<>();
    try (JarFile jar = new JarFile(BUNDLE)) {
      for (JarEntry entry : (Iterable<JarEntry>) jar.stream()::iterator) {
        if (entry.getName().startsWith("META-INF/maven/io.netty/")
            && entry.getName().endsWith("/pom.properties")) {
          Properties properties = new Properties();
          try (InputStream in = jar.getInputStream(entry)) {
            properties.load(in);
          }
          versions.add(entry.getName() + "=" + properties.getProperty("version"));
        }
      }
    }
    assertFalse(versions.isEmpty(), "No Netty module found in " + BUNDLE.getName());
    for (String version : versions) {
      assertTrue(version.endsWith("=" + NETTY_VERSION), version);
    }
  }

  @Test
  void testShadedSocketFactoryLinks() throws Exception {
    // hadoop-aws links this class against the Apache HTTP client shaded by the AWS SDK bundle. The
    // build relocates that package to the Apache HTTP client of the single SDK modules.
    try (URLClassLoader loader = newLoader()) {
      Class<?> factory =
          loader.loadClass("org.apache.hadoop.fs.s3a.impl.ConfigureShadedAWSSocketFactory");
      Class<?> mode =
          loader.loadClass(
              "org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory$SSLChannelMode");
      Class<?> builderClass =
          loader.loadClass("software.amazon.awssdk.http.apache.ApacheHttpClient$Builder");
      Object builder =
          loader
              .loadClass("software.amazon.awssdk.http.apache.ApacheHttpClient")
              .getMethod("builder")
              .invoke(null);
      Object instance = factory.getConstructor().newInstance();
      // Fails with NoClassDefFoundError when the shaded HTTP client cannot be resolved.
      factory
          .getMethod("configureSocketFactory", builderClass, mode)
          .invoke(instance, builder, mode.getField("Default_JSSE").get(null));
    }
  }

  @Test
  void testS3AFileSystemInitializes() throws Exception {
    try (URLClassLoader loader = newLoader()) {
      ClassLoader original = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(loader);
      try {
        Class<?> confClass = loader.loadClass("org.apache.hadoop.conf.Configuration");
        Object conf = confClass.getConstructor(boolean.class).newInstance(false);
        String[][] settings = {
          {
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
          },
          {"fs.s3a.access.key", "access-key"},
          {"fs.s3a.secret.key", "secret-key"},
          {"fs.s3a.endpoint", "http://127.0.0.1:1"},
          {"fs.s3a.endpoint.region", "us-east-1"},
          {"fs.s3a.path.style.access", "true"},
          {"fs.s3a.connection.ssl.enabled", "false"},
          {"fs.s3a.bucket.probe", "0"}
        };
        for (String[] setting : settings) {
          confClass
              .getMethod("set", String.class, String.class)
              .invoke(conf, setting[0], setting[1]);
        }
        Class<?> fsClass = loader.loadClass("org.apache.hadoop.fs.s3a.S3AFileSystem");
        Object fs = fsClass.getConstructor().newInstance();
        fsClass
            .getMethod("initialize", URI.class, confClass)
            .invoke(fs, URI.create("s3a://bucket/"), conf);
        try {
          assertEquals("s3a", fsClass.getMethod("getScheme").invoke(fs));
        } finally {
          fsClass.getMethod("close").invoke(fs);
        }
      } finally {
        Thread.currentThread().setContextClassLoader(original);
      }
    }
  }

  private static URLClassLoader newLoader() throws Exception {
    URL[] urls = {
      BUNDLE.toURI().toURL(),
      LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation()
    };
    // The ordinary test classpath also contains unshaded Hadoop and AWS SDK classes.
    return new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());
  }
}
