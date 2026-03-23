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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClosableHiveCatalog is a wrapper class to wrap Iceberg HiveCatalog to do some clean-up work like
 * closing resources.
 */
public class ClosableJdbcCatalog extends JdbcCatalog implements Closeable, SupportsKerberos {

    private static final Logger LOGGER= LoggerFactory.getLogger(ClosableJdbcCatalog.class);

    private KerberosClient kerberosClient;

    private Configuration hadoopConf;

    public ClosableJdbcCatalog() {
        super();
    }

    public ClosableJdbcCatalog(
            Function<Map<String, String>, FileIO> ioBuilder,
            Function<Map<String, String>, JdbcClientPool> clientPoolBuilder,
            boolean initializeCatalogTables) {
        super(ioBuilder, clientPoolBuilder, initializeCatalogTables);
    }

    /**
     * Initialize the ClosableHiveCatalog with the given input name and properties.
     *
     * <p>Note: This method can only be called once as it will create new client pools.
     *
     * @param inputName name of the catalog
     * @param properties properties for the catalog
     */
    @Override
    public void initialize(String inputName, Map<String, String> properties) {
        super.initialize(inputName, properties);

        AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
        if (authenticationConfig.isKerberosAuth()) {
            this.kerberosClient = initKerberosClient();
        }
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public void setHadoopConf(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public void close() {
        if (kerberosClient != null) {
            try {
                kerberosClient.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close KerberosClient", e);
            }
        }
    }

    @Override
    public <R> R doKerberosOperations(Executable<R> executable) throws Throwable {
        Map<String, String> properties = this.properties();
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

        return realUser.doAs(
                (PrivilegedExceptionAction<R>)
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

    private KerberosClient initKerberosClient() {
        try {
            KerberosClient kerberosClient = new KerberosClient(this.properties(), this.getHadoopConf());
            // catalog_uuid always exists for Gravitino managed catalogs, `0` is just a fallback value.
            String catalogUUID = properties().getOrDefault("catalog_uuid", "0");
            File keytabFile = kerberosClient.saveKeyTabFileFromUri(Long.parseLong(catalogUUID));
            kerberosClient.login(keytabFile.getAbsolutePath());
            return kerberosClient;
        } catch (IOException e) {
            throw new RuntimeException("Failed to login with kerberos", e);
        }
    }
}
