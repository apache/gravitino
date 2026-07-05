/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.cos.fs;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link COSCredentialsProvider}, the Hadoop-side bridge that turns Gravitino-vended
 * credentials into {@code com.qcloud.cos.auth.COSCredentials} instances. The stub {@link
 * StubGravitinoFileSystemCredentialsProvider} below is wired via {@code
 * fs.gvfs.credential.provider} so that {@code FileSystemUtils.getGvfsCredentialProvider} can
 * reflectively instantiate it during the {@link COSCredentialsProvider} constructor.
 */
public class TestCOSCredentialsProvider {

  private static final URI COS_URI = URI.create("cosn://test-bucket-1250000000/");

  @BeforeEach
  void resetStub() {
    StubGravitinoFileSystemCredentialsProvider.reset();
  }

  @AfterEach
  void clearStub() {
    StubGravitinoFileSystemCredentialsProvider.reset();
  }

  @Test
  void testRefreshWithSecretKeyCredential() {
    StubGravitinoFileSystemCredentialsProvider.nextCredentials =
        new Credential[] {new COSSecretKeyCredential("test-ak", "test-sk")};

    COSCredentialsProvider provider = new COSCredentialsProvider(COS_URI, newStubConf());

    COSCredentials credentials = provider.getCredentials();

    Assertions.assertNotNull(credentials);
    Assertions.assertTrue(credentials instanceof BasicCOSCredentials);
    Assertions.assertEquals("test-ak", credentials.getCOSAccessKeyId());
    Assertions.assertEquals("test-sk", credentials.getCOSSecretKey());
  }

  @Test
  void testRefreshThrowsWhenNoSuitableCredential() {
    // No COSSecretKeyCredential in the array -> COSUtils#getSuitableCredential returns null
    // -> COSCredentialsProvider#refresh throws.
    StubGravitinoFileSystemCredentialsProvider.nextCredentials = new Credential[] {};

    COSCredentialsProvider provider = new COSCredentialsProvider(COS_URI, newStubConf());

    RuntimeException ex = Assertions.assertThrows(RuntimeException.class, provider::getCredentials);
    Assertions.assertTrue(
        ex.getMessage() != null && ex.getMessage().contains("No suitable credential"),
        "Expected message about no suitable credential, but got: " + ex.getMessage());
  }

  @Test
  void testGetCredentialsIsCachedForNonExpiringCredential() {
    // COSSecretKeyCredential#expireTimeInMs() returns 0, so expirationTime stays at
    // Long.MAX_VALUE and subsequent getCredentials() calls must return the cached
    // BasicCOSCredentials without re-asking the upstream provider.
    StubGravitinoFileSystemCredentialsProvider.nextCredentials =
        new Credential[] {new COSSecretKeyCredential("ak1", "sk1")};

    COSCredentialsProvider provider = new COSCredentialsProvider(COS_URI, newStubConf());

    COSCredentials first = provider.getCredentials();
    COSCredentials second = provider.getCredentials();
    COSCredentials third = provider.getCredentials();

    Assertions.assertSame(first, second);
    Assertions.assertSame(second, third);
    Assertions.assertEquals(
        1,
        StubGravitinoFileSystemCredentialsProvider.callCount.get(),
        "Static, non-expiring credentials should only be fetched from the upstream provider once");
  }

  private static Configuration newStubConf() {
    Configuration conf = new Configuration(false);
    conf.set(
        GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
        StubGravitinoFileSystemCredentialsProvider.class.getName());
    return conf;
  }

  /**
   * Test stub for {@link GravitinoFileSystemCredentialsProvider}. Must be {@code public} with a
   * public no-arg constructor because {@code FileSystemUtils#getGvfsCredentialProvider}
   * instantiates it reflectively.
   *
   * <p>Tests communicate with the stub through the {@link #nextCredentials} static field, because
   * the reflective construction prevents passing values via the constructor.
   */
  public static final class StubGravitinoFileSystemCredentialsProvider
      implements GravitinoFileSystemCredentialsProvider {

    static volatile Credential[] nextCredentials = new Credential[] {};
    static final AtomicInteger callCount = new AtomicInteger(0);

    private Configuration conf;

    public StubGravitinoFileSystemCredentialsProvider() {}

    static void reset() {
      nextCredentials = new Credential[] {};
      callCount.set(0);
    }

    @Override
    public Credential[] getCredentials() {
      callCount.incrementAndGet();
      return nextCredentials;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
