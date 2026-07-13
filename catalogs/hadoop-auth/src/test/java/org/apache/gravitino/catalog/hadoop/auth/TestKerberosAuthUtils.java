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

package org.apache.gravitino.catalog.hadoop.auth;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

public class TestKerberosAuthUtils {

  @TempDir private File tempDir;

  @Test
  public void testCheckPrincipalAndGetRealm() {
    Assertions.assertEquals(
        "EXAMPLE.COM", KerberosAuthUtils.checkPrincipalAndGetRealm("service/host@EXAMPLE.COM"));
    Assertions.assertEquals(
        "EXAMPLE.COM", KerberosAuthUtils.checkPrincipalAndGetRealm(" service/host@EXAMPLE.COM "));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KerberosAuthUtils.checkPrincipalAndGetRealm(""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> KerberosAuthUtils.checkPrincipalAndGetRealm("service/host"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> KerberosAuthUtils.checkPrincipalAndGetRealm("service/host@REALM@EXTRA"));
  }

  @Test
  public void testFetchKeytabFromLocalUri() throws Exception {
    File source = new File(tempDir, "source.keytab");
    File destination = new File(tempDir, "destination.keytab");
    Files.writeString(source.toPath(), "keytab-content");

    File fetched =
        KerberosAuthUtils.fetchKeytabFromUri(
            source.toURI().toString(),
            destination,
            1,
            false /* allowHdfsKeytabUri */,
            null /* hadoopConf */);

    Assertions.assertEquals(destination.getAbsolutePath(), fetched.getAbsolutePath());
    Assertions.assertTrue(Files.isSymbolicLink(destination.toPath()));
    Assertions.assertEquals(source.toPath(), Files.readSymbolicLink(destination.toPath()));
  }

  @Test
  public void testFetchKeytabRejectsHdfsWhenDisabled() {
    File destination = new File(tempDir, "destination.keytab");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                KerberosAuthUtils.fetchKeytabFromUri(
                    "hdfs://namenode/keytab",
                    destination,
                    1,
                    false /* allowHdfsKeytabUri */,
                    null /* hadoopConf */));

    Assertions.assertTrue(exception.getMessage().contains("HDFS"));
  }

  @Test
  public void testConcurrentFetchKeytabCreatesParentDirectoryOnce() throws Exception {
    File source = new File(tempDir, "source.keytab");
    Files.writeString(source.toPath(), "keytab-content");
    String sourceUri = source.toURI().toString();

    int threads = 4;
    int iterations = 200;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int i = 0; i < iterations; i++) {
        File parent = new File(tempDir, "race-" + i + "/keytabs");
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Future<File>> futures = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
          File destination = new File(parent, "destination-" + t + ".keytab");
          futures.add(
              executor.submit(
                  () -> {
                    barrier.await();
                    return KerberosAuthUtils.fetchKeytabFromUri(
                        sourceUri,
                        destination,
                        1,
                        false /* allowHdfsKeytabUri */,
                        null /* hadoopConf */);
                  }));
        }
        for (Future<File> future : futures) {
          File fetched = future.get();
          Assertions.assertTrue(fetched.exists());
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testConfigureKrb5ConfSetsSystemProperty() {
    String hadoopKrb5ConfKey = "gravitino.test.krb5.conf";
    String systemKrb5ConfKey = "java.security.krb5.conf.test";
    String krb5ConfPath = new File(tempDir, "krb5.conf").getAbsolutePath();
    Configuration configuration = new Configuration(false);
    configuration.set(hadoopKrb5ConfKey, krb5ConfPath);

    String originalValue = System.getProperty(systemKrb5ConfKey);
    try {
      KerberosAuthUtils.configureKrb5Conf(configuration, hadoopKrb5ConfKey, systemKrb5ConfKey);
      Assertions.assertEquals(krb5ConfPath, System.getProperty(systemKrb5ConfKey));
    } finally {
      if (originalValue == null) {
        System.clearProperty(systemKrb5ConfKey);
      } else {
        System.setProperty(systemKrb5ConfKey, originalValue);
      }
    }
  }

  @Test
  public void testConfigureKrb5ConfRejectsInvalidKeys() {
    Configuration configuration = new Configuration(false);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> KerberosAuthUtils.configureKrb5Conf(configuration, " ", "java.security.krb5.conf"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KerberosAuthUtils.configureKrb5Conf(configuration, "hadoop.security.krb5.conf", null));
  }

  @Test
  public void testStartTicketRefreshRejectsInvalidArguments() {
    UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> KerberosAuthUtils.startTicketRefresh(ugi, 0, LoggerFactory.getLogger(getClass())));
  }

  @Test
  public void testLoginRejectsNullLoginMode() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                KerberosAuthUtils.login(
                    "service/host@EXAMPLE.COM",
                    new File(tempDir, "missing.keytab").getAbsolutePath(),
                    new Configuration(false),
                    null));

    Assertions.assertTrue(exception.getMessage().contains("loginMode"));
  }

  @Test
  public void testStartTicketRefreshReturnsDedicatedExecutorPerCall() {
    UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    ScheduledExecutorService firstRefresh =
        KerberosAuthUtils.startTicketRefresh(ugi, 60, LoggerFactory.getLogger(getClass()));
    ScheduledExecutorService secondRefresh =
        KerberosAuthUtils.startTicketRefresh(ugi, 60, LoggerFactory.getLogger(getClass()));

    try {
      Assertions.assertNotSame(firstRefresh, secondRefresh);
      Assertions.assertFalse(firstRefresh.isShutdown());
      Assertions.assertFalse(secondRefresh.isShutdown());
      Assertions.assertTrue(
          Thread.getAllStackTraces().keySet().stream()
              .anyMatch(
                  candidate ->
                      candidate.isDaemon()
                          && candidate.getName().startsWith("kerberos-ticket-refresh-")));
    } finally {
      firstRefresh.shutdownNow();
      secondRefresh.shutdownNow();
    }

    Assertions.assertTrue(firstRefresh.isShutdown());
    Assertions.assertTrue(secondRefresh.isShutdown());
  }
}
