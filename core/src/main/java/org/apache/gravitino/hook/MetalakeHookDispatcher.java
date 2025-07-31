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

import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code MetalakeHookDispatcher} is a decorator for {@link MetalakeDispatcher} that not only
 * delegates metalake operations to the underlying metalake dispatcher but also executes some hook
 * operations before or after the underlying operations.
 */
public class MetalakeHookDispatcher implements MetalakeDispatcher {
  private final MetalakeDispatcher dispatcher;

  public MetalakeHookDispatcher(MetalakeDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public Metalake[] listMetalakes() {
    return dispatcher.listMetalakes();
  }

  @Override
  public Metalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    return dispatcher.loadMetalake(ident);
  }

  @Override
  public Metalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    Metalake metalake = dispatcher.createMetalake(ident, comment, properties);

    // Add the creator to the metalake
    AccessControlDispatcher accessControlDispatcher =
        GravitinoEnv.getInstance().accessControlDispatcher();
    if (accessControlDispatcher != null) {
      accessControlDispatcher.addUser(ident.name(), PrincipalUtils.getCurrentUserName());
    }

    // Set the creator as owner of the metalake.
    OwnerDispatcher ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerDispatcher != null) {
      ownerDispatcher.setOwner(
          ident.name(),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.METALAKE),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return metalake;
  }

  @Override
  public Metalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    Metalake alterMetalake = dispatcher.alterMetalake(ident, changes);
    MetalakeChange.RenameMetalake lastRenameChange = null;
    for (MetalakeChange change : changes) {
      if (change instanceof MetalakeChange.RenameMetalake) {
        lastRenameChange = (MetalakeChange.RenameMetalake) change;
      }
    }
    if (lastRenameChange != null) {
      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.METALAKE, lastRenameChange.getNewName());
    }
    return alterMetalake;
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, MetalakeInUseException {
    return dispatcher.dropMetalake(ident, force);
  }

  @Override
  public void enableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    dispatcher.enableMetalake(ident);
  }

  @Override
  public void disableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    dispatcher.disableMetalake(ident);
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    // For metalake, we don't clear all the privileges of catalog authorization plugin.
    // we just remove metalake.
    return dispatcher.dropMetalake(ident);
  }

  @Override
  public boolean metalakeExists(NameIdentifier ident) {
    return dispatcher.metalakeExists(ident);
  }
}
