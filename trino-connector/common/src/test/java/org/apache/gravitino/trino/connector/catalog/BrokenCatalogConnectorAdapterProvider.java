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
package org.apache.gravitino.trino.connector.catalog;

import org.apache.gravitino.trino.connector.GravitinoConfig;

/** Fails to link once its methods run, like a provider built against another Trino version. */
public class BrokenCatalogConnectorAdapterProvider implements CatalogConnectorAdapterProvider {

  public static final String PROVIDER = "jdbc-broken";

  @Override
  public String provider() {
    throw new NoClassDefFoundError("io/trino/spi/Missing");
  }

  @Override
  public CatalogConnectorAdapter createAdapter(GravitinoConfig config) {
    return null;
  }
}
