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
package org.apache.gravitino.iceberg.service;

import java.io.Closeable;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies S3 initialization using only the experimental server's production dependencies. */
class TestExperimentalAwsRuntime {

  @Test
  void testS3FileIOCreatesOutputFileWithoutContactingS3() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = newRuntimeLoader()) {
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> fileIOClass = loader.loadClass("org.apache.iceberg.aws.s3.S3FileIO");
      try (Closeable fileIO = (Closeable) fileIOClass.getConstructor().newInstance()) {
        fileIOClass
            .getMethod("initialize", Map.class)
            .invoke(
                fileIO,
                Map.of(
                    "client.region", "us-east-1",
                    "s3.access-key-id", "test-access-key",
                    "s3.secret-access-key", "test-secret-key"));
        // This builds the SDK and HTTP clients but does not write any data or make a request.
        Object output =
            fileIOClass
                .getMethod("newOutputFile", String.class)
                .invoke(fileIO, "s3://test-bucket/table/metadata/test.metadata.json");
        Assertions.assertNotNull(output);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testCredentialGeneratorsCanResolveTheirRuntimeDependencies() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = newRuntimeLoader()) {
      Thread.currentThread().setContextClassLoader(loader);
      Assertions.assertEquals(
          1,
          Collections.list(
                  loader.getResources(
                      "software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.class"))
              .size(),
          "The experimental server must have exactly one AWS SDK implementation");
      Class<?> generator = loader.loadClass("org.apache.gravitino.s3.credential.S3TokenGenerator");
      Assertions.assertNotNull(generator.getConstructor().newInstance());
      // Reflective generator loading also resolves the SDK types used in method signatures.
      Assertions.assertTrue(generator.getDeclaredMethods().length > 0);
      Class<?> provider =
          loader.loadClass("org.apache.gravitino.catalog.glue.GravitinoGlueCredentialsProvider");
      Class<?> credentialsProvider =
          loader.loadClass("software.amazon.awssdk.auth.credentials.AwsCredentialsProvider");
      Assertions.assertTrue(credentialsProvider.isAssignableFrom(provider));
      Assertions.assertNotNull(
          loader.loadClass("software.amazon.awssdk.policybuilder.iam.IamPolicy"));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private URLClassLoader newRuntimeLoader() {
    String classpath = System.getProperty("experimentalRuntimeClasspath");
    Assertions.assertNotNull(classpath);
    URL[] urls =
        Arrays.stream(classpath.split(File.pathSeparator))
            .map(File::new)
            .map(
                file -> {
                  try {
                    return file.toURI().toURL();
                  } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(e);
                  }
                })
            .toArray(URL[]::new);
    return new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());
  }
}
