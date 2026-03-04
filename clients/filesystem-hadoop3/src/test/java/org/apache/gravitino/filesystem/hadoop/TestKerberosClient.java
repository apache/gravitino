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
package org.apache.gravitino.filesystem.hadoop;

import static org.apache.gravitino.server.authentication.KerberosConfig.KEYTAB;
import static org.apache.gravitino.server.authentication.KerberosConfig.PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpResponse.response;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.gravitino.Config;
import org.apache.gravitino.server.authentication.KerberosAuthenticator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;

public class TestKerberosClient extends TestGvfsBase {

  @BeforeAll
  public static void setup() {
    try {
      KdcServerBase.initKeyTab();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
        KdcServerBase.getClientPrincipal());
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY,
        KdcServerBase.getKeytabFile());
  }

  @AfterAll
  public static void teardown() {
    KdcServerBase.stopKdc();
  }

  @Test
  public void testAuthConfigs() {
    // init conf
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "testAuthConfigs", true);
    Configuration configuration = new Configuration();
    configuration.set(
        String.format(
            "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
        "true");
    configuration.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    configuration.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY, serverUri());

    // set auth type, but do not set other configs
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          FileSystem fs = managedFilesetPath.getFileSystem(configuration);
          // Trigger lazy initialization
          fs.exists(managedFilesetPath);
        });

    // set not exist keytab path
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY,
        "file://tmp/test.keytab");
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          FileSystem fs = managedFilesetPath.getFileSystem(configuration);
          // Trigger lazy initialization
          fs.exists(managedFilesetPath);
        });
  }

  @Test
  public void testAuthWithPrincipalAndKeytabNormally() throws Exception {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, "testAuthWithPrincipalAndKeytabNormally", true);
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
    Config config = new Config(false) {};
    config.set(PRINCIPAL, KdcServerBase.getServerPrincipal());
    config.set(KEYTAB, KdcServerBase.getKeytabFile());
    kerberosAuthenticator.initialize(config);

    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
        KdcServerBase.getClientPrincipal());
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY,
        KdcServerBase.getKeytabFile());

    // mock load metalake with principal and keytab
    String testMetalake = "test_kerberos_normally";
    HttpRequest mockRequest =
        HttpRequest.request("/api/metalakes/" + testMetalake)
            .withMethod(Method.GET.name())
            .withQueryStringParameters(Collections.emptyMap());
    mockServer()
        .when(mockRequest, Times.unlimited())
        .respond(
            httpRequest -> {
              List<Header> headers = httpRequest.getHeaders().getEntries();
              for (Header header : headers) {
                if (header.getName().equalsIgnoreCase("Authorization")) {
                  byte[] tokenValue =
                      header.getValues().get(0).getValue().getBytes(StandardCharsets.UTF_8);
                  kerberosAuthenticator.authenticateToken(tokenValue);
                }
              }
              return response().withStatusCode(HttpStatus.SC_OK);
            });
    Path newPath = new Path(managedFilesetPath.toString().replace(metalakeName, testMetalake));
    // Should auth successfully
    newPath.getFileSystem(configuration);
  }

  @Test
  public void testAuthWithInvalidInfo() throws Exception {
    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(
            catalogName, schemaName, "testAuthWithInvalidInfo", true);
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
    Config config = new Config(false) {};
    config.set(PRINCIPAL, KdcServerBase.getServerPrincipal());
    config.set(KEYTAB, KdcServerBase.getKeytabFile());
    kerberosAuthenticator.initialize(config);

    // test with invalid principal and keytab
    Configuration conf1 = new Configuration(conf);
    conf1.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
        "invalid@EXAMPLE.COM");
    conf1.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY,
        KdcServerBase.getKeytabFile());

    String testMetalake = "test_invalid";
    HttpRequest mockRequest =
        HttpRequest.request("/api/metalakes/" + testMetalake)
            .withMethod(Method.GET.name())
            .withQueryStringParameters(Collections.emptyMap());
    mockServer()
        .when(mockRequest, Times.unlimited())
        .respond(
            httpRequest -> {
              List<Header> headers = httpRequest.getHeaders().getEntries();
              for (Header header : headers) {
                if (header.getName().equalsIgnoreCase("Authorization")) {
                  byte[] tokenValue =
                      header.getValues().get(0).getValue().getBytes(StandardCharsets.UTF_8);
                  kerberosAuthenticator.authenticateToken(tokenValue);
                }
              }
              return response().withStatusCode(HttpStatus.SC_OK);
            });
    Path newPath = new Path(managedFilesetPath.toString().replace(metalakeName, testMetalake));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> {
          FileSystem fs = newPath.getFileSystem(conf1);
          // Trigger lazy initialization
          fs.exists(newPath);
        });

    // test with principal and invalid keytab
    File invalidKeytabFile =
        new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString());
    if (invalidKeytabFile.exists()) {
      invalidKeytabFile.delete();
    }
    invalidKeytabFile.createNewFile();

    Configuration conf2 = new Configuration(conf);
    conf2.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
        KdcServerBase.getClientPrincipal());
    conf2.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY,
        invalidKeytabFile.getAbsolutePath());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> {
          FileSystem fs = newPath.getFileSystem(conf2);
          // Trigger lazy initialization
          fs.exists(newPath);
        });
    invalidKeytabFile.delete();

    // test with principal and no keytab
    Configuration conf3 = new Configuration(conf);
    conf3.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
        KdcServerBase.getClientPrincipal());
    // remove keytab configuration
    conf3.unset(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY);
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> {
          FileSystem fs = newPath.getFileSystem(conf3);
          // Trigger lazy initialization
          fs.exists(newPath);
        });
  }
}
