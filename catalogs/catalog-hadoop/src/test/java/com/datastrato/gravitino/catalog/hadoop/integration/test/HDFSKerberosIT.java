/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hadoop.integration.test;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class HDFSKerberosIT {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSKerberosIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static UserGroupInformation clientUGI;

  @BeforeAll
  public static void setup() throws IOException {
    containerSuite.startKerberosHiveContainer();

    containerSuite
        .getKerberosHiveContainer()
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", "/tmp/client.keytab");

    containerSuite
        .getKerberosHiveContainer()
        .getContainer()
        .copyFileFromContainer("/etc/krb5.conf", "/tmp/krb5.conf_tmp");

    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();

    String content =
        FileUtils.readFileToString(new File("/tmp/krb5.conf_tmp"), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File("/tmp/krb5.conf"), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}", content);

    System.setProperty("java.security.krb5.conf", "/tmp/krb5.conf");
    System.setProperty("sun.security.krb5.debug", "true");
  }

  @AfterAll
  public static void tearDown() {
    // Reset the UGI
    UserGroupInformation.reset();
  }

  @Test
  public void testKerberosHDFS() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(conf);
    clientUGI =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            CLIENT_PRINCIPAL, "/tmp/client.keytab");
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

  private static String defaultBaseLocation() {
    return String.format(
        "hdfs://%s:%d/user/",
        containerSuite.getKerberosHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
