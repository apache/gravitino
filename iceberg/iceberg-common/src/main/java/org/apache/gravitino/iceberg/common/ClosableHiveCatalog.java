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

package org.apache.gravitino.iceberg.common;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClosableHiveCatalog is a wrapper class to wrap Iceberg HiveCatalog to do some clean-up work like
 * closing resources.
 */
public class ClosableHiveCatalog extends HiveCatalog implements Closeable, SupportsKerberos {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosableHiveCatalog.class);

  @Getter private final List<Closeable> resources = Lists.newArrayList();

  private KerberosClient kerberosClient;

  public ClosableHiveCatalog() {
    super();
  }

  public void addResource(Closeable resource) {
    resources.add(resource);
  }

  public void setKerberosClient(KerberosClient kerberosClient) {
    this.kerberosClient = kerberosClient;
  }

  @Override
  public void close() throws IOException {
    if (kerberosClient != null) {
      try {
        kerberosClient.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close KerberosClient", e);
      }
    }

    // Do clean up work here. We need a mechanism to close the HiveCatalog; however, HiveCatalog
    // doesn't implement the Closeable interface.
    resources.forEach(
        resource -> {
          try {
            if (resource != null) {
              resource.close();
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to close resource: {}", resource, e);
          }
        });
  }

  @Override
  public <R> R doKerberosOperations(Map<String, String> properties, Executable<R> executable)
      throws Throwable {

    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);

    final String finalPrincipalName;
    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();

    if (!proxyKerberosPrincipalName.contains("@")) {
      finalPrincipalName =
          String.format("%s@%s", proxyKerberosPrincipalName, kerberosClient.getRealm());
    } else {
      finalPrincipalName = proxyKerberosPrincipalName;
    }

    UserGroupInformation realUser =
        authenticationConfig.isImpersonationEnabled()
            ? UserGroupInformation.createProxyUser(
                finalPrincipalName, kerberosClient.getLoginUser())
            : kerberosClient.getLoginUser();
    try {
      ClientPool<IMetaStoreClient, TException> newClientPool =
          (ClientPool<IMetaStoreClient, TException>) FieldUtils.readField(this, "clients", true);
      kerberosClient
          .getLoginUser()
          .doAs(
              (PrivilegedExceptionAction<Void>)
                  () -> {
                    String token =
                        newClientPool.run(
                            client ->
                                client.getDelegationToken(
                                    finalPrincipalName,
                                    kerberosClient.getLoginUser().getShortUserName()));

                    Token<DelegationTokenIdentifier> delegationToken = new Token<>();
                    delegationToken.decodeFromUrlString(token);
                    realUser.addToken(delegationToken);
                    return null;
                  });
    } catch (Exception e) {
      throw new RuntimeException("Failed to get delegation token", e);
    }
    return (R)
        realUser.doAs(
            (PrivilegedExceptionAction<Object>)
                () -> {
                  try {
                    return executable.execute();
                  } catch (Throwable e) {
                    if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                      throw (RuntimeException) e;
                    }
                    throw new RuntimeException("Failed to invoke method", e);
                  }
                });
  }

  public void resetHiveClientPool(String principalName, UserGroupInformation realUser) {
    try {
      ClientPool<IMetaStoreClient, TException> newClientPool =
          (ClientPool<IMetaStoreClient, TException>) FieldUtils.readField(this, "clients", true);

      kerberosClient
          .getLoginUser()
          .doAs(
              (PrivilegedExceptionAction<Void>)
                  () -> {
                    String token =
                        newClientPool.run(
                            client ->
                                client.getDelegationToken(
                                    principalName,
                                    kerberosClient.getLoginUser().getShortUserName()));

                    Token<DelegationTokenIdentifier> delegationToken = new Token<>();
                    delegationToken.decodeFromUrlString(token);
                    realUser.addToken(delegationToken);
                    return null;
                  });
    } catch (Exception e) {
      throw new RuntimeException("Failed to get delegation token", e);
    }
  }
}
