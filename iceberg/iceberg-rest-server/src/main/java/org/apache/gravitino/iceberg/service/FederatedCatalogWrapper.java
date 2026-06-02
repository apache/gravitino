/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * A {@link CatalogWrapperForREST} for a federated Iceberg REST catalog (the underlying catalog is a
 * {@link RESTCatalog}).
 *
 * <p>Federation-specific behavior is expressed through polymorphic overrides instead of {@code
 * instanceof RESTCatalog} checks scattered across the base class. Table operations are routed to
 * the base class {@code *Internal} methods so client-facing FileIO and credential properties are
 * extracted from the remote catalog's {@code table.io()}. Credentials are vended by the remote
 * catalog, so this wrapper never injects Gravitino-generated credentials.
 */
public class FederatedCatalogWrapper extends CatalogWrapperForREST {

  /**
   * Creates a federated wrapper.
   *
   * @param catalogName the catalog name.
   * @param config the Iceberg catalog configuration (backend {@code rest}).
   */
  public FederatedCatalogWrapper(String catalogName, IcebergConfig config) {
    super(catalogName, config);
  }

  @Override
  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    // The remote REST catalog vends its own credentials, so the requestCredential flag is not used
    // here; FileIO-derived client config is extracted by createTableInternal.
    return createTableInternal(namespace, request);
  }

  @Override
  public LoadTableResponse loadTable(
      TableIdentifier identifier, boolean requestCredential, CredentialPrivilege privilege) {
    return loadTableInternal(identifier);
  }

  @Override
  public LoadTableResponse registerTable(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    return registerTableInternal(namespace, request);
  }

  @Override
  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    return tableUpdateInternal(tableIdentifier, updateTableRequest);
  }

  @Override
  Map<String, String> buildCatalogConfigToClients() {
    Map<String, String> merged = ((RESTCatalog) getCatalog()).properties();
    return filterCatalogConfigForClients(merged != null ? merged : Collections.emptyMap());
  }
}
