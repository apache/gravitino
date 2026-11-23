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

package org.apache.gravitino.catalog.fileset.integration.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class HDFSKerberosIT {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSKerberosIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static UserGroupInformation clientUGI;

  private static String keytabPath;

  @BeforeAll
  public static void setup() throws IOException {
    containerSuite.startKerberosHiveContainer();

    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();

    // Copy the keytab and krb5.conf from the container
    keytabPath = file.getAbsolutePath() + "/client.keytab";
    containerSuite
        .getKerberosHiveContainer()
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", keytabPath);

    String krb5TmpPath = file.getAbsolutePath() + "/krb5.conf_tmp";
    String krb5Path = file.getAbsolutePath() + "/krb5.conf";
    containerSuite
        .getKerberosHiveContainer()
        .getContainer()
        .copyFileFromContainer("/etc/krb5.conf", krb5TmpPath);

    // Modify the krb5.conf and change the kdc and admin_server to the container IP
    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();
    String content = FileUtils.readFileToString(new File(krb5TmpPath), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}", content);

    System.setProperty("java.security.krb5.conf", krb5Path);
    KerberosName.resetDefaultRealm();
    System.setProperty("sun.security.krb5.debug", "true");
  }

  @AfterAll
  public static void tearDown() {
    // Reset the UGI
    UserGroupInformation.reset();

    // Clean up the kerberos configuration
    System.clearProperty("java.security.krb5.conf");
    System.clearProperty("sun.security.krb5.debug");
  }

  @Test
  public void testKerberosHDFS() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(conf);
    clientUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(CLIENT_PRINCIPAL, keytabPath);
    PrivilegedAction<?> action =
        (PrivilegedAction)
            () -> {
              try {
                FileSystem fs = FileSystem.get(conf);
                Path path = new Path("/");
                Assertions.assertTrue(fs.exists(path));
                return null;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            };

    clientUGI.doAs(action);

    // Clear UGI, It will throw exception
    UserGroupInformation.reset();
    Exception e = Assertions.assertThrows(Exception.class, action::run);
    Assertions.assertInstanceOf(AccessControlException.class, e.getCause());
  }

  /**
   * Logs in from the keytab and logs in again with the Kerberos client of Hadoop 3.4 (Kerby 2.0.3
   * in hadoop-auth), the way a long-running client renews its ticket. Each relogin must obtain a
   * new TGT, and the HDFS access must keep working with it.
   */
  @Test
  public void testKerberosHDFSKeytabRelogin() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set("hadoop.security.authentication", "kerberos");
    // Hadoop skips a relogin within this period after the last login (60 seconds by default).
    conf.setInt("hadoop.kerberos.min.seconds.before.relogin", 0);
    UserGroupInformation.reset();
    UserGroupInformation.setConfiguration(conf);
    try {
      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(CLIENT_PRINCIPAL, keytabPath);
      Assertions.assertTrue(ugi.isFromKeytab());
      Assertions.assertTrue(ugi.hasKerberosCredentials());
      Assertions.assertEquals(CLIENT_PRINCIPAL, ugi.getUserName());
      KerberosTicket initialTgt = getTgt(ugi);
      assertRootExists(ugi, conf);

      // A forced relogin always logs in again with the keytab.
      ugi.forceReloginFromKeytab();
      KerberosTicket forcedTgt = getTgt(ugi);
      Assertions.assertNotSame(initialTgt, forcedTgt, "forceReloginFromKeytab got no new TGT");
      assertRootExists(ugi, conf);

      // The conditional relogin only logs in again when the TGT is due for a refresh. Make every
      // TGT due, so that the renewal path runs.
      UserGroupInformation.setShouldRenewImmediatelyForTests(true);
      ugi.checkTGTAndReloginFromKeytab();
      KerberosTicket renewedTgt = getTgt(ugi);
      Assertions.assertNotSame(
          forcedTgt, renewedTgt, "checkTGTAndReloginFromKeytab got no new TGT for a due TGT");
      assertRootExists(ugi, conf);
    } finally {
      UserGroupInformation.setShouldRenewImmediatelyForTests(false);
    }
  }

  // UserGroupInformation#getSubject is protected, so read the Subject of the UGI inside doAs.
  @SuppressWarnings("removal")
  private static KerberosTicket getTgt(UserGroupInformation ugi) throws Exception {
    Subject subject =
        ugi.doAs(
            (PrivilegedExceptionAction<Subject>)
                () -> Subject.getSubject(AccessController.getContext()));
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    List<KerberosTicket> tgts =
        tickets.stream()
            .filter(t -> t.getServer().getName().startsWith("krbtgt/"))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, tgts.size(), "Expect exactly one TGT of the client");
    return tgts.get(0);
  }

  private static void assertRootExists(UserGroupInformation ugi, Configuration conf)
      throws Exception {
    boolean exists =
        ugi.doAs(
            (PrivilegedExceptionAction<Boolean>)
                () -> {
                  try (FileSystem fs = FileSystem.get(conf)) {
                    return fs.exists(new Path("/"));
                  }
                });
    Assertions.assertTrue(exists);
  }

  private static String defaultBaseLocation() {
    return String.format(
        "hdfs://%s:%d/user/",
        containerSuite.getKerberosHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
