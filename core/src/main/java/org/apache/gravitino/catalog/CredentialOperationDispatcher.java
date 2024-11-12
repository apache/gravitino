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

package org.apache.gravitino.catalog;

import java.util.List;
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.SupportsCredentialOperations;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class CredentialOperationDispatcher extends OperationDispatcher {

  public CredentialOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  public List<Credential> getCredentials(NameIdentifier identifier) {
    return doWithCatalog(
        NameIdentifierUtil.getCatalogIdentifier(identifier),
        c -> loadCredentials(c.catalog(), identifier),
        NoSuchCatalogException.class);
  }

  private List<Credential> loadCredentials(BaseCatalog catalog, NameIdentifier identifier) {
    CatalogOperations catalogOperations = catalog.ops();
    if (!(catalogOperations instanceof SupportsCredentialOperations)) {
      throw new NotSupportedException("Not support credential vending for the catalog");
    }
    SupportsCredentialOperations supportsCredentialOperation =
        (SupportsCredentialOperations) catalogOperations;

    return supportsCredentialOperation.getCredentials(identifier);
  }
}
