/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hadoop.integration.test;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.KerberosizedHDFSContainer;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class KerberosizedHDFSTest {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosizedHDFSTest.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String CLIENT_PRINCIPAL = "client@EXAMPLE.COM";
  private static UserGroupInformation clientUGI;

  @BeforeAll
  public static void setup() throws IOException {
    containerSuite.startHDFSContainer();

    containerSuite
        .getHdfsContainer()
        .getContainer()
        .copyFileFromContainer("/tmp/client.keytab", "/tmp/client.keytab");

    containerSuite
        .getHdfsContainer()
        .getContainer()
        .copyFileFromContainer("/etc/krb5.conf", "/tmp/krb5.conf_tmp");

    String ip = containerSuite.getHdfsContainer().getContainerIpAddress();

    String content =
        FileUtils.readFileToString(new File("/tmp/krb5.conf_tmp"), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File("/tmp/krb5.conf"), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}", content);

    System.setProperty("java.security.krb5.conf", "/tmp/krb5.conf");
    System.setProperty("sun.security.krb5.debug", "true");
  }

  @Test
  public void testKerberoizeHDFS() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(conf);
    clientUGI =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            CLIENT_PRINCIPAL, "/tmp/client.keytab");
    clientUGI.doAs(
        (PrivilegedAction)
            () -> {
              try {
                FileSystem fs = FileSystem.get(conf);
                Path path = new Path("/user/client");
                Assertions.assertTrue(fs.exists(path));

                path = new Path("/user/client/test1");
                fs.mkdirs(path);

                FileStatus status = fs.getFileStatus(path);
                Assertions.assertEquals(755, status.getPermission().toOctal());
                return null;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static String defaultBaseLocation() {
    return String.format(
        "hdfs://%s:%d/user/",
        containerSuite.getHdfsContainer().getContainerIpAddress(),
        KerberosizedHDFSContainer.HDFS_DEFAULTFS_PORT);
  }
}
