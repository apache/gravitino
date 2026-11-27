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
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies S3 initialization using only the experimental server's production dependencies. */
class TestExperimentalAwsRuntime {

  private static final String AWS_CREDENTIALS_PROVIDER =
      "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider";
  private static final String APACHE5_HTTP_CLIENT_BUILDER =
      "software.amazon.awssdk.http.apache5.Apache5HttpClient$Builder";
  private static final String S3_TOKEN_GENERATOR =
      "org.apache.gravitino.s3.credential.S3TokenGenerator";

  @Test
  void testS3FileIOCreatesOutputFileWithoutContactingS3() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = newRuntimeLoader()) {
      Thread.currentThread().setContextClassLoader(loader);
      assertS3FileIO(loader);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testCredentialGeneratorsCanResolveTheirRuntimeDependencies() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = newRuntimeLoader()) {
      Thread.currentThread().setContextClassLoader(loader);
      initializeTokenProvider(loader);
      assertCredentialDependencies(loader);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void testCredentialDelegationAndS3WithIsolatedClassLoader() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader serverLoader =
        new URLClassLoader(
            runtimeUrls("serverRuntimeClasspath"), ClassLoader.getPlatformClassLoader())) {
      Assertions.assertThrows(
          ClassNotFoundException.class, () -> serverLoader.loadClass(AWS_CREDENTIALS_PROVIDER));
      Assertions.assertThrows(
          ClassNotFoundException.class, () -> serverLoader.loadClass(S3_TOKEN_GENERATOR));
      Thread.currentThread().setContextClassLoader(serverLoader);
      try (IsolatedClassLoader isolated =
          new IsolatedClassLoader(
              Arrays.asList(runtimeUrls("experimentalRuntimeClasspath")),
              Collections.emptyList(),
              Collections.emptyList())) {
        isolated.withClassLoader(
            loader -> {
              Assertions.assertSame(loader, Thread.currentThread().getContextClassLoader());
              Assertions.assertSame(
                  serverLoader.loadClass(
                      "org.apache.gravitino.credential.CredentialProviderDelegator"),
                  loader.loadClass("org.apache.gravitino.credential.CredentialProviderDelegator"));
              // Exercise the delegator before directly loading its generator or any AWS SDK class.
              initializeTokenProvider(loader);
              Assertions.assertSame(loader, loader.loadClass(S3_TOKEN_GENERATOR).getClassLoader());
              Assertions.assertSame(
                  loader, loader.loadClass(AWS_CREDENTIALS_PROVIDER).getClassLoader());
              Assertions.assertSame(
                  loader, loader.loadClass(APACHE5_HTTP_CLIENT_BUILDER).getClassLoader());
              assertCredentialDependencies(loader);
              assertS3FileIO(loader);
              return null;
            });
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private void assertS3FileIO(ClassLoader loader) throws Exception {
    Assertions.assertNotNull(loader.loadClass(APACHE5_HTTP_CLIENT_BUILDER));
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
      // Construct the SDK and HTTP clients without writing data or making an S3 request.
      Object output =
          fileIOClass
              .getMethod("newOutputFile", String.class)
              .invoke(fileIO, "s3://test-bucket/table/metadata/test.metadata.json");
      Assertions.assertNotNull(output);
    }
  }

  private void assertCredentialDependencies(ClassLoader loader) throws Exception {
    Assertions.assertEquals(
        1,
        Collections.list(loader.getResources(AWS_CREDENTIALS_PROVIDER.replace('.', '/') + ".class"))
            .size(),
        "The experimental server must have exactly one AWS SDK implementation");
    Class<?> generator = loader.loadClass(S3_TOKEN_GENERATOR);
    Assertions.assertEquals(
        "org.apache.gravitino.credential.S3TokenCredential",
        generator
            .getMethod(
                "generate", loader.loadClass("org.apache.gravitino.credential.CredentialContext"))
            .getReturnType()
            .getName());
    Class<?> provider =
        loader.loadClass("org.apache.gravitino.catalog.glue.GravitinoGlueCredentialsProvider");
    Assertions.assertTrue(loader.loadClass(AWS_CREDENTIALS_PROVIDER).isAssignableFrom(provider));

    // Exercise the separately versioned policy builder against the bundled SDK utilities.
    Class<?> policyClass = loader.loadClass("software.amazon.awssdk.policybuilder.iam.IamPolicy");
    Object policy =
        policyClass
            .getMethod("fromJson", String.class)
            .invoke(
                null,
                "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
                    + "\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::test-bucket/*\"}]}");
    String serialized = (String) policyClass.getMethod("toJson").invoke(policy);
    Assertions.assertTrue(serialized.contains("s3:GetObject"));
  }

  private void initializeTokenProvider(ClassLoader loader) throws Exception {
    Class<?> providerClass = loader.loadClass("org.apache.gravitino.s3.credential.S3TokenProvider");
    Class<?> delegator =
        loader.loadClass("org.apache.gravitino.credential.CredentialProviderDelegator");
    Assertions.assertSame(
        delegator, providerClass.getMethod("initialize", Map.class).getDeclaringClass());
    try (Closeable provider = (Closeable) providerClass.getConstructor().newInstance()) {
      // This invokes CredentialProviderDelegator.loadGenerator with the caller's TCCL.
      providerClass
          .getMethod("initialize", Map.class)
          .invoke(
              provider,
              Map.of(
                  "s3-role-arn", "arn:aws:iam::123456789012:role/test-role",
                  "s3-region", "us-east-1",
                  "s3-access-key-id", "test-access-key",
                  "s3-secret-access-key", "test-secret-key"));
      Assertions.assertEquals(
          "s3-token", providerClass.getMethod("credentialType").invoke(provider));
    }
  }

  private URLClassLoader newRuntimeLoader() {
    return new URLClassLoader(
        runtimeUrls("experimentalRuntimeClasspath"), ClassLoader.getPlatformClassLoader());
  }

  private URL[] runtimeUrls(String propertyName) {
    String classpath = System.getProperty(propertyName);
    Assertions.assertNotNull(classpath);
    return Arrays.stream(classpath.split(File.pathSeparator))
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
  }
}
