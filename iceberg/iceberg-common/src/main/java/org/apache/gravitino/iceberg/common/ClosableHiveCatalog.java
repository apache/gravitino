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
import java.util.List;
import lombok.Getter;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClosableHiveCatalog is a wrapper class to wrap Iceberg HiveCatalog to do some clean-up work like
 * closing resources.
 */
public class ClosableHiveCatalog extends HiveCatalog implements Closeable, SupportsKerberos {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosableHiveCatalog.class);

  @Getter private final List<Closeable> resources = Lists.newArrayList();

  @SuppressWarnings("unused")
  private ClosableHiveCatalog proxy;

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
  public KerberosClient getKerberosClient() {
    return kerberosClient;
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
}
