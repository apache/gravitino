/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive.integration.test;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.client.KerberosTokenProvider;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class HiveUserAuthenticationIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(HiveUserAuthenticationIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @SuppressWarnings("unused")
  private static final String CLIENT_PRINCIPAL = "cli@HADOOPKRB";

  private static String clientKeytabFile = "/tmp/client.keytab";

  private static final String SERVER_PRINCIPAL = "HTTP/_HOST@HADOOPKRB";
  private static String serverKeytabFile = "/tmp/server.keytab";

  @SuppressWarnings("unused")
  private static final String KRB5_CONF_FILE = "/tmp/krb5.conf";

  @SuppressWarnings("unused")
  private static UserGroupInformation clientUGI;

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    containerSuite.startKerberosHiveContainer();

    // Prepare kerberos related-config;
    prepareKerberosConfig();

    // Config kerberos configuration for Gravitino server
    addKerberosConfig();
    // GET KDC info from container

    // Copy the keytab and krb5.conf from the container

    // Modify the krb5.conf and change the kdc and admin_server to the container IP

    // Set the system properties for kerberos

    AbstractIT.startIntegrationTest();
  }

  private static void prepareKerberosConfig() throws IOException {
    // Copy the keytab and krb5.conf from the container
    containerSuite
        .getKerberosHiveContainer()
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", clientKeytabFile);

    containerSuite
        .getKerberosHiveContainer()
        .getContainer()
        .copyFileFromContainer("/etc/krb5.conf", "/tmp/krb5.conf_tmp");

    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();

    // Modify the krb5.conf and change the kdc and admin_server to the container IP
    String content =
        FileUtils.readFileToString(new File("/tmp/krb5.conf_tmp"), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File("/tmp/krb5.conf"), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}", content);
    System.setProperty("java.security.krb5.conf", "/tmp/krb5.conf");
    System.setProperty("sun.security.krb5.debug", "true");
  }

  private static void addKerberosConfig() {
    AbstractIT.customConfigs.put("gravitino.authenticator", "kerberos");
    AbstractIT.customConfigs.put("gravitino.authenticator.kerberos.principal", SERVER_PRINCIPAL);
    AbstractIT.customConfigs.put("gravitino.authenticator.kerberos.keytab", serverKeytabFile);
  }

  @Test
  public void testUserAuthentication() {
    // Test user authentication
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(clientKeytabFile)
            .withKeyTabFile(new File(clientKeytabFile))
            .build();

    GravitinoAdminClient client =
        GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    GravitinoMetalake[] metalakes = client.listMetalakes();

    System.out.println("xxxx");
    System.out.println(metalakes.length);
    LOG.info("end of the test");
  }
}
