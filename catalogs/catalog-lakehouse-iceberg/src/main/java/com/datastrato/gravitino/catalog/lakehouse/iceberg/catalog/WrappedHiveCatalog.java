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

package com.datastrato.gravitino.catalog.lakehouse.iceberg.catalog;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.authentication.kerberos.KerberosClient;
import java.io.Closeable;
import java.io.IOException;
import org.apache.iceberg.hive.HiveCatalog;

/**
 * A wrapper class for HiveCatalog to support kerberos authentication. We can also make HiveCatalog
 * as a generic type and pass it as a parameter to the constructor.
 */
public class WrappedHiveCatalog extends HiveCatalog implements Closeable {

  private KerberosClient kerberosClient;

  public WrappedHiveCatalog() {
    super();
  }

  public void setKerberosClient(KerberosClient kerberosClient) {
    this.kerberosClient = kerberosClient;
  }

  @Override
  public void close() throws IOException {
    // Do clean up work here. We need a mechanism to close the HiveCatalog; however, HiveCatalog
    // doesn't implement the Closeable interface.

    if (kerberosClient != null) {
      kerberosClient.close();
    }
  }
}
