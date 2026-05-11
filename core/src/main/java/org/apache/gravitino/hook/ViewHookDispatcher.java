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
package org.apache.gravitino.hook;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Decorates {@link ViewDispatcher} with ownership and authorization hooks, matching {@link
 * TableHookDispatcher}.
 */
public class ViewHookDispatcher implements ViewDispatcher {
  private final ViewDispatcher dispatcher;

  public ViewHookDispatcher(ViewDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listViews(namespace);
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    return dispatcher.loadView(ident);
  }

  @Override
  public boolean viewExists(NameIdentifier ident) {
    return dispatcher.viewExists(ident);
  }

  @Override
  public View createView(
      NameIdentifier ident,
      @Nullable String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    View view =
        dispatcher.createView(
            ident, comment, columns, representations, defaultCatalog, defaultSchema, properties);
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      NameIdentifier normalizedIdent =
          CapabilityHelpers.applyCapabilities(
              ident, Capability.Scope.VIEW, GravitinoEnv.getInstance().catalogManager());
      ownerManager.setOwner(
          normalizedIdent.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(normalizedIdent, Entity.EntityType.VIEW),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return view;
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    ViewChange.RenameView lastRename = null;
    List<String> locations = null;
    for (ViewChange change : changes) {
      if (change instanceof ViewChange.RenameView) {
        lastRename = (ViewChange.RenameView) change;
      }
    }
    if (lastRename != null) {
      locations = AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.VIEW);
    }
    View altered = dispatcher.alterView(ident, changes);
    if (lastRename != null) {
      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.VIEW, lastRename.getNewName(), locations);
    }
    return altered;
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.VIEW);
    boolean dropped = dispatcher.dropView(ident);
    AuthorizationUtils.authorizationPluginRemovePrivileges(
        ident, Entity.EntityType.VIEW, locations);
    return dropped;
  }
}
