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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;

import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.storage.IdGenerator;

public class FilesetOperationDispatcher extends OperationDispatcher implements FilesetDispatcher {
  /**
   * Creates a new FilesetOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for fileset operations.
   * @param store The EntityStore instance to be used for fileset operations.
   * @param idGenerator The IdGenerator instance to be used for fileset operations.
   */
  public FilesetOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  /**
   * List the filesets in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of fileset identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(namespace.levels()),
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
                c -> c.doWithFilesetOps(f -> f.listFilesets(namespace)),
                NoSuchSchemaException.class));
  }

  /**
   * Load fileset metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A fileset identifier.
   * @return The fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   */
  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Fileset fileset =
        TreeLockUtils.doWithTreeLock(
            ident,
            LockType.READ,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithFilesetOps(f -> f.loadFileset(ident)),
                    NoSuchFilesetException.class));
    // Currently we only support maintaining the Fileset in the Gravitino's store.
    return EntityCombinedFileset.of(fileset)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::filesetPropertiesMetadata,
                fileset.properties()));
  }

  @Override
  public FileInfo[] listFiles(NameIdentifier ident, String locationName, String subPath)
      throws NoSuchFilesetException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () ->
            doWithCatalog(
                catalogIdent,
                c -> c.doWithFilesetFileOps(f -> f.listFiles(ident, locationName, subPath)),
                NoSuchFilesetException.class));
  }

  /**
   * Create a fileset metadata in the catalog.
   *
   * <p>If the type of the fileset object is "MANAGED", the underlying storageLocations can be
   * empty, and Gravitino will manage the storage location based on the locations of the schema.
   *
   * <p>If the type of the fileset object is "EXTERNAL", the underlying storageLocations must be
   * set.
   *
   * @param ident A fileset identifier.
   * @param comment The comment of the fileset.
   * @param type The type of the fileset.
   * @param storageLocations The location names and corresponding storage locations of the fileset.
   * @param properties The properties of the fileset.
   * @return The created fileset metadata
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FilesetAlreadyExistsException If the fileset already exists.
   */
  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.filesetPropertiesMetadata(), properties);
                  return null;
                }),
        IllegalArgumentException.class);
    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    Fileset createdFileset =
        TreeLockUtils.doWithTreeLock(
            NameIdentifier.of(ident.namespace().levels()),
            LockType.WRITE,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c ->
                        c.doWithFilesetOps(
                            f ->
                                f.createMultipleLocationFileset(
                                    ident, comment, type, storageLocations, updatedProperties)),
                    NoSuchSchemaException.class,
                    FilesetAlreadyExistsException.class));
    return EntityCombinedFileset.of(createdFileset)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::filesetPropertiesMetadata,
                createdFileset.properties()));
  }

  /**
   * Apply the {@link FilesetChange change} to a fileset in the catalog.
   *
   * <p>Implementation may reject the change. If any change is rejected, no changes should be
   * applied to the fileset.
   *
   * <p>The {@link FilesetChange.RenameFileset} change will only update the fileset name, the
   * underlying storage location for managed fileset will not be renamed.
   *
   * @param ident A fileset identifier.
   * @param changes The changes to apply to the fileset.
   * @return The altered fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::filesetPropertiesMetadata, changes);
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    boolean containsRenameFileset =
        Arrays.stream(changes).anyMatch(c -> c instanceof FilesetChange.RenameFileset);
    NameIdentifier nameIdentifierForLock =
        containsRenameFileset ? NameIdentifier.of(ident.namespace().levels()) : ident;

    Fileset alteredFileset =
        TreeLockUtils.doWithTreeLock(
            nameIdentifierForLock,
            LockType.WRITE,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithFilesetOps(f -> f.alterFileset(ident, changes)),
                    NoSuchFilesetException.class,
                    IllegalArgumentException.class));

    return EntityCombinedFileset.of(alteredFileset)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::filesetPropertiesMetadata,
                alteredFileset.properties()));
  }

  /**
   * Drop a fileset from the catalog.
   *
   * <p>The underlying files will be deleted if this fileset type is managed, otherwise, only the
   * metadata will be dropped.
   *
   * @param ident A fileset identifier.
   * @return true If the fileset is dropped, false the fileset did not exist.
   */
  @Override
  public boolean dropFileset(NameIdentifier ident) {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ident.namespace().levels()),
        LockType.WRITE,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithFilesetOps(f -> f.dropFileset(ident)),
                NonEmptyEntityException.class));
  }

  /**
   * Get the actual location of a file or directory based on the storage location of Fileset and the
   * sub path by the location name.
   *
   * @param ident A fileset identifier.
   * @param subPath The sub path to the file or directory.
   * @param locationName The location name.
   * @return The actual location of the file or directory.
   * @throws NoSuchFilesetException If the fileset does not exist.
   * @throws NoSuchLocationNameException If the location name does not exist.
   */
  @Override
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithFilesetOps(f -> f.getFileLocation(ident, subPath, locationName)),
                NonEmptyEntityException.class));
  }
}
