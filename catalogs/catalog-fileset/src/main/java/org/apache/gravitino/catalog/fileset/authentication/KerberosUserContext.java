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

package org.apache.gravitino.catalog.fileset.authentication;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosClient;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosUserContext extends UserContext {
  public static final Logger LOGGER = LoggerFactory.getLogger(KerberosUserContext.class);

  private UserGroupInformation userGroupInformation;
  private boolean enableUserImpersonation;
  private String kerberosRealm;
  private final String keytab;

  private KerberosClient kerberosClient;

  KerberosUserContext(boolean enableUserImpersonation, String keytabPath) {
    this.enableUserImpersonation = enableUserImpersonation;
    this.keytab = keytabPath;
  }

  public void setEnableUserImpersonation(boolean enableUserImpersonation) {
    this.enableUserImpersonation = enableUserImpersonation;
  }

  public synchronized void initKerberos(
      Map<String, String> properties, Configuration configuration, boolean refreshCredentials) {
    configuration.set(
        HADOOP_SECURITY_AUTHENTICATION,
        AuthenticationMethod.KERBEROS.name().toLowerCase(Locale.ROOT));
    try {
      UserGroupInformation.setConfiguration(configuration);
      KerberosClient client = new KerberosClient(properties, configuration, refreshCredentials);
      // Add the kerberos client to the closable to close resources.
      this.kerberosClient = client;
      File keytabFile = client.saveKeyTabFileFromUri(keytab);
      this.kerberosRealm = client.login(keytabFile.getAbsolutePath());
      this.userGroupInformation = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with Kerberos", e);
    }
  }

  @Override
  UserGroupInformation createProxyUser() {
    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentUserName();
    if (!proxyKerberosPrincipalName.contains("@")) {
      proxyKerberosPrincipalName =
          String.format("%s@%s", proxyKerberosPrincipalName, kerberosRealm);
    }

    return UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, getUser());
  }

  @Override
  UserGroupInformation getUser() {
    return userGroupInformation;
  }

  @Override
  boolean enableUserImpersonation() {
    return enableUserImpersonation;
  }

  @Override
  public void close() throws IOException {
    if (kerberosClient != null) {
      kerberosClient.close();
    }

    if (keytab == null) {
      return;
    }

    File file = new File(keytab);
    if (file.exists()) {
      boolean isDeleted = file.delete();
      if (!isDeleted) {
        LOGGER.error("Failed to delete file: {}", keytab);
      }
    }
  }

  public KerberosUserContext deepCopy() {
    KerberosUserContext copy = new KerberosUserContext(enableUserImpersonation, keytab);
    copy.userGroupInformation = userGroupInformation;
    copy.kerberosRealm = kerberosRealm;
    return copy;
  }
}
