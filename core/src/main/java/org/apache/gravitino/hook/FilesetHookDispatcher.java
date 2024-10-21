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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code FilesetHookDispatcher} is a decorator for {@link FilesetDispatcher} that not only
 * delegates fileset operations to the underlying fileset dispatcher but also executes some hook
 * operations before or after the underlying operations.
 */
public class FilesetHookDispatcher implements FilesetDispatcher {
  private final FilesetDispatcher dispatcher;

  public FilesetHookDispatcher(FilesetDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listFilesets(namespace);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    return dispatcher.loadFileset(ident);
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(
        ident.namespace().level(0), PrincipalUtils.getCurrentUserName());

    Fileset fileset = dispatcher.createFileset(ident, comment, type, storageLocation, properties);

    // Set the creator as the owner of the fileset.
    OwnerManager ownerManager = GravitinoEnv.getInstance().ownerManager();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.FILESET),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return fileset;
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    return dispatcher.alterFileset(ident, changes);
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    return dispatcher.dropFileset(ident);
  }

  @Override
  public boolean filesetExists(NameIdentifier ident) {
    return dispatcher.filesetExists(ident);
  }

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath)
      throws NoSuchFilesetException {
    return dispatcher.getFileLocation(ident, subPath);
  }
}
