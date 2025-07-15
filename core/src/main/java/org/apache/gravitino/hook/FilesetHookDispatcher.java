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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.FileInfo;
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
  public FileInfo[] listFiles(NameIdentifier ident, String locationName, String subPath)
      throws NoSuchFilesetException, IOException {
    return dispatcher.listFiles(ident, locationName, subPath);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    return dispatcher.loadFileset(ident);
  }

  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(
        ident.namespace().level(0), PrincipalUtils.getCurrentUserName());

    Fileset fileset =
        dispatcher.createMultipleLocationFileset(
            ident, comment, type, storageLocations, properties);

    // Set the creator as the owner of the fileset.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
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
    Fileset alteredFileset = dispatcher.alterFileset(ident, changes);
    FilesetChange.RenameFileset lastRenameChange = null;
    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.RenameFileset) {
        lastRenameChange = (FilesetChange.RenameFileset) change;
      }
    }
    if (lastRenameChange != null) {
      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.FILESET, lastRenameChange.getNewName());
    }

    return alteredFileset;
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.FILESET);
    boolean dropped = dispatcher.dropFileset(ident);
    AuthorizationUtils.authorizationPluginRemovePrivileges(
        ident, Entity.EntityType.FILESET, locations);
    return dropped;
  }

  @Override
  public boolean filesetExists(NameIdentifier ident) {
    return dispatcher.filesetExists(ident);
  }

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException, NoSuchLocationNameException {
    return dispatcher.getFileLocation(ident, subPath, locationName);
  }
}
