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

package org.apache.gravitino.credential;

import java.util.List;
import javax.ws.rs.NotAuthorizedException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.OperationDispatcher;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Get credentials with the specific catalog classloader. */
public class CredentialOperationDispatcher extends OperationDispatcher {

  public CredentialOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  public List<Credential> getCredentials(NameIdentifier identifier) {
    CredentialPrivilege privilege =
        getCredentialPrivilege(PrincipalUtils.getCurrentUserName(), identifier);
    return doWithCatalog(
        NameIdentifierUtil.getCatalogIdentifier(identifier),
        catalogWrapper ->
            catalogWrapper.doWithCredentialOps(
                credentialOps -> credentialOps.getCredentials(identifier, privilege)),
        NoSuchCatalogException.class);
  }

  @SuppressWarnings("UnusedVariable")
  private CredentialPrivilege getCredentialPrivilege(String user, NameIdentifier identifier)
      throws NotAuthorizedException {
    // TODO: will implement in another PR
    return CredentialPrivilege.WRITE;
  }
}
