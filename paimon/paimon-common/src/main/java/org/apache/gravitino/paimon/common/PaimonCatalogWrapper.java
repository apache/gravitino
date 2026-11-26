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
package org.apache.gravitino.paimon.common;

import com.google.common.util.concurrent.Striped;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.paimon.common.PaimonCatalogException.Resource;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.apache.paimon.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a Paimon {@link Catalog} (filesystem, JDBC or Hive backend) and adapts it to the Paimon
 * REST catalog protocol.
 *
 * <p>Table metadata (schema, options, uuid) is persisted by the wrapped catalog; the uuid is kept
 * as a table option ({@link #TABLE_UUID_OPTION}) so it stays stable across server restarts. Data
 * file IO and snapshot files live on the table location under the warehouse. Snapshot writes run
 * under the backend's table lock and return {@code false} when the snapshot already exists so the
 * client can regenerate and retry. Tables created outside REST persist their identity in a file
 * under the table location, so deleting and recreating the table also replaces its identity.
 */
public class PaimonCatalogWrapper implements Closeable {

  /** Table option that stores the configured table uuid. */
  public static final String TABLE_UUID_OPTION = "rest.uuid";

  private static final Logger LOG = LoggerFactory.getLogger(PaimonCatalogWrapper.class);

  private static final Striped<java.util.concurrent.locks.Lock> LOCAL_TABLE_LOCKS =
      Striped.lazyWeakLock(1024);

  private static final String PATH_OPTION = "path";
  private static final String TABLE_UUID_FILE = ".rest-uuid";

  private static final String EARLIEST_SNAPSHOT = "EARLIEST";
  private static final String LATEST_SNAPSHOT = "LATEST";

  private final Catalog catalog;
  private final Options catalogOptions;
  private final String catalogName;

  /**
   * Creates a wrapper over a new Paimon catalog built from the given options.
   *
   * @param catalogName the configured name of this catalog
   * @param catalogOptions the catalog configuration (metastore, warehouse, uri, credentials...)
   */
  public PaimonCatalogWrapper(String catalogName, Options catalogOptions) {
    this.catalogName = catalogName;
    this.catalogOptions = PaimonCatalogOptions.fromProperties(catalogOptions.toMap());
    this.catalog = CatalogFactory.createCatalog(CatalogContext.create(this.catalogOptions));
    LOG.info(
        "Created Paimon catalog '{}' with metastore {}",
        catalogName,
        this.catalogOptions.get(CatalogOptions.METASTORE));
  }

  PaimonCatalogWrapper(String catalogName, Options catalogOptions, Catalog catalog) {
    this.catalogName = catalogName;
    this.catalogOptions = catalogOptions;
    this.catalog = catalog;
  }

  /**
   * Returns the configured catalog name.
   *
   * @return the catalog name
   */
  public String name() {
    return catalogName;
  }

  /**
   * Returns the raw catalog options used to build the client-facing config response.
   *
   * @return the catalog options
   */
  public Options catalogOptions() {
    return catalogOptions;
  }

  // ------------------------------------------------------------------
  // Database operations
  // ------------------------------------------------------------------

  /**
   * Lists all database names in ascending order.
   *
   * @return the sorted database names
   */
  public List<String> listDatabases() {
    return catalog.listDatabases().stream().sorted().collect(Collectors.toList());
  }

  /**
   * Creates a database.
   *
   * @param name the database name
   * @param options the database options
   */
  public void createDatabase(String name, Map<String, String> options) {
    try {
      catalog.createDatabase(name, false, options);
    } catch (Catalog.DatabaseAlreadyExistException e) {
      throw PaimonCatalogException.alreadyExists(
          Resource.DATABASE, name, "Database %s already exists.", name);
    }
  }

  /**
   * Loads a database.
   *
   * @param name the database name
   * @return the database metadata
   */
  public Database getDatabase(String name) {
    try {
      return catalog.getDatabase(name);
    } catch (Catalog.DatabaseNotExistException e) {
      throw databaseNotExist(name);
    }
  }

  /**
   * Drops a database. The database must be empty.
   *
   * @param name the database name
   */
  public void dropDatabase(String name) {
    try {
      catalog.dropDatabase(name, false /* cascade */, false);
    } catch (Catalog.DatabaseNotExistException e) {
      throw databaseNotExist(name);
    } catch (Catalog.DatabaseNotEmptyException e) {
      throw PaimonCatalogException.invalid(
          "Database %s is not empty. It must be empty before it is dropped.", name);
    }
  }

  /**
   * Alters database properties by removing and updating options.
   *
   * @param name the database name
   * @param removals the option keys to remove
   * @param updates the option keys to set
   * @return the removed keys, updated keys and keys that were missing
   */
  public AlterDatabaseResult alterDatabase(
      String name, List<String> removals, Map<String, String> updates) {
    try {
      Database database = catalog.getDatabase(name);
      Map<String, String> current = database.options();
      List<String> removed =
          removals == null
              ? Collections.emptyList()
              : removals.stream().filter(current::containsKey).collect(Collectors.toList());
      List<String> missing =
          removals == null
              ? Collections.emptyList()
              : removals.stream()
                  .filter(key -> !current.containsKey(key))
                  .collect(Collectors.toList());
      List<PropertyChange> changes = new ArrayList<>();
      if (removals != null) {
        removals.forEach(key -> changes.add(PropertyChange.removeProperty(key)));
      }
      if (updates != null) {
        updates.forEach((key, value) -> changes.add(PropertyChange.setProperty(key, value)));
      }
      catalog.alterDatabase(name, changes, false);
      List<String> updated =
          updates == null
              ? Collections.emptyList()
              : updates.keySet().stream().sorted().collect(Collectors.toList());
      return new AlterDatabaseResult(removed, updated, missing);
    } catch (Catalog.DatabaseNotExistException e) {
      throw databaseNotExist(name);
    }
  }

  // ------------------------------------------------------------------
  // Table operations
  // ------------------------------------------------------------------

  /**
   * Lists all table names of a database in ascending order.
   *
   * @param database the database name
   * @return the sorted table names
   */
  public List<String> listTables(String database) {
    try {
      return catalog.listTables(database).stream().sorted().collect(Collectors.toList());
    } catch (Catalog.DatabaseNotExistException e) {
      throw databaseNotExist(database);
    }
  }

  /**
   * Lists tables matching an optional Paimon table type.
   *
   * @param databaseName the database name
   * @param tableType the type to match, or null or empty for all types
   * @return the matching table names
   */
  public List<String> listTables(String databaseName, @Nullable String tableType) {
    List<String> tables = listTables(databaseName);
    if (tableType == null || tableType.isEmpty()) {
      return tables;
    }
    List<String> matching = new ArrayList<>();
    for (String name : tables) {
      Identifier identifier = Identifier.create(databaseName, name);
      try {
        if (tableType.equals(
            catalog.getTable(identifier).options().getOrDefault("type", "table"))) {
          matching.add(name);
        }
      } catch (Catalog.TableNotExistException e) {
        // A table can disappear after listing its name. Keep the remaining matches.
      }
    }
    return matching;
  }

  /**
   * Creates a table. The table uuid is generated and persisted as a table option. External tables
   * (custom path) are not supported yet.
   *
   * @param identifier the table identifier
   * @param schema the table schema
   */
  public void createTable(Identifier identifier, Schema schema) {
    if (schema.options().containsKey(PATH_OPTION)) {
      throw PaimonCatalogException.unsupported(
          "External tables (custom path) are not supported by the Paimon REST server yet.");
    }
    schema.options().keySet().forEach(PaimonCatalogWrapper::validateMutableProperty);
    Map<String, String> options = new HashMap<>(schema.options());
    options.put(TABLE_UUID_OPTION, UUID.randomUUID().toString());
    Schema withUuid =
        new Schema(
            schema.fields(),
            schema.partitionKeys(),
            schema.primaryKeys(),
            options,
            schema.comment());
    try {
      catalog.createTable(identifier, withUuid, false);
    } catch (Catalog.DatabaseNotExistException e) {
      throw databaseNotExist(identifier.getDatabaseName());
    } catch (Catalog.TableAlreadyExistException e) {
      throw PaimonCatalogException.alreadyExists(
          Resource.TABLE,
          identifier.getObjectName(),
          "Table %s already exists.",
          identifier.getFullName());
    }
  }

  /**
   * Loads a table and its persistent identity. The uuid is read from the table options or persisted
   * at the table location for tables created outside the REST server.
   *
   * @param identifier the table identifier
   * @return the table and its identity
   */
  public LoadedTable getTable(Identifier identifier) {
    FileStoreTable table = getFileTable(identifier);
    try {
      String uuid = loadTableUuid(table);
      if (uuid != null) {
        return new LoadedTable(table, uuid);
      }
      // All branches share the identity file at the table root.
      return withTableLock(
          Identifier.create(identifier.getDatabaseName(), identifier.getTableName()),
          table,
          () -> {
            FileStoreTable current = getFileTable(identifier);
            String existing = loadTableUuid(current);
            if (existing == null) {
              Path uuidPath = new Path(current.location(), TABLE_UUID_FILE);
              current.fileIO().tryToWriteAtomic(uuidPath, UUID.randomUUID().toString());
              existing = current.fileIO().readFileUtf8(uuidPath);
            }
            return new LoadedTable(current, existing);
          });
    } catch (PaimonCatalogException e) {
      throw e;
    } catch (Exception e) {
      throw PaimonCatalogException.failure(
          "Failed to initialize identity of table %s: %s",
          identifier.getFullName(), e.getMessage());
    }
  }

  /**
   * Alters a table schema.
   *
   * @param identifier the table identifier
   * @param changes the schema changes
   */
  public void alterTable(Identifier identifier, List<SchemaChange> changes) {
    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetOption) {
        validateMutableProperty(((SchemaChange.SetOption) change).key());
      } else if (change instanceof SchemaChange.RemoveOption) {
        validateMutableProperty(((SchemaChange.RemoveOption) change).key());
      }
    }
    try {
      catalog.alterTable(identifier, changes, false);
    } catch (Catalog.TableNotExistException e) {
      throw tableNotExist(identifier);
    } catch (Catalog.ColumnNotExistException e) {
      throw PaimonCatalogException.notFound(
          Resource.COLUMN,
          e.column(),
          "Column %s does not exist in table %s.",
          e.column(),
          identifier.getFullName());
    } catch (Catalog.ColumnAlreadyExistException e) {
      throw PaimonCatalogException.alreadyExists(
          Resource.COLUMN,
          e.column(),
          "Column %s already exists in table %s.",
          e.column(),
          identifier.getFullName());
    }
  }

  /**
   * Drops a table together with its data (Paimon semantics; drop equals purge).
   *
   * @param identifier the table identifier
   */
  public void dropTable(Identifier identifier) {
    try {
      catalog.dropTable(identifier, false);
    } catch (Catalog.TableNotExistException e) {
      throw tableNotExist(identifier);
    }
  }

  /**
   * Renames a table.
   *
   * @param source the source identifier
   * @param destination the destination identifier
   */
  public void renameTable(Identifier source, Identifier destination) {
    try {
      catalog.renameTable(source, destination, false);
    } catch (Catalog.TableNotExistException e) {
      throw tableNotExist(source);
    } catch (Catalog.TableAlreadyExistException e) {
      throw PaimonCatalogException.alreadyExists(
          Resource.TABLE,
          destination.getObjectName(),
          "Table %s already exists.",
          destination.getFullName());
    }
  }

  // ------------------------------------------------------------------
  // Snapshots and commits
  // ------------------------------------------------------------------

  /**
   * Commits a snapshot produced by a client writer. The commit is an atomic create-if-not-exists of
   * the snapshot file at the table location. A conflicting snapshot returns {@code false} so the
   * writer can regenerate and retry.
   *
   * @param identifier the table identifier, possibly branch-qualified
   * @param tableId the uuid of the table as known by the client
   * @param baseSnapshotUuid the snapshot UUID used by the writer, or null for legacy writers
   * @param snapshot the snapshot to commit
   * @return true when the snapshot was committed
   */
  public boolean commitTable(
      Identifier identifier, String tableId, @Nullable String baseSnapshotUuid, Snapshot snapshot) {
    FileStoreTable table = getFileTable(identifier);
    try {
      return withTableLock(
          identifier,
          table,
          () -> {
            // Reload inside the lock: a table may have been dropped and recreated by another
            // catalog since the request arrived. Never commit using cached metadata or identity.
            FileStoreTable current = getFileTable(identifier);
            if (tableId == null || !tableId.equals(loadTableUuid(current))) {
              throw tableNotExist(identifier);
            }
            SnapshotManager manager = current.snapshotManager();
            Snapshot latest = manager.latestSnapshot();
            long nextId = latest == null ? Snapshot.FIRST_SNAPSHOT_ID : latest.id() + 1;
            if (snapshot.id() != nextId
                || (baseSnapshotUuid != null
                    && (latest == null || !baseSnapshotUuid.equals(latest.uuid())))) {
              return false;
            }
            Path snapshotPath = manager.snapshotPath(snapshot.id());
            FileIO fileIO = current.fileIO();
            // Object-store rename is not necessarily atomic. The existence check and snapshot
            // creation must stay in the same backend lock, as in Paimon's RenamingSnapshotCommit.
            if (fileIO.exists(snapshotPath)
                || !fileIO.tryToWriteAtomic(snapshotPath, snapshot.toJson())) {
              return false;
            }
            manager.commitLatestHint(snapshot.id());
            return true;
          });
    } catch (PaimonCatalogException e) {
      throw e;
    } catch (Exception e) {
      throw PaimonCatalogException.failure(
          "Failed to commit snapshot %s to table %s: %s",
          snapshot.id(), identifier.getFullName(), e.getMessage() == null ? "" : e.getMessage());
    }
  }

  /**
   * Loads the latest snapshot of a table.
   *
   * @param identifier the table identifier
   * @return the latest snapshot, or empty when the table has no snapshot yet
   */
  public Optional<Snapshot> loadLatestSnapshot(Identifier identifier) {
    FileStoreTable table = getFileTable(identifier);
    return Optional.ofNullable(table.snapshotManager().latestSnapshot());
  }

  /**
   * Loads the latest snapshot together with statistics of its live data files.
   *
   * @param identifier the native table identifier, possibly branch-qualified
   * @return snapshot statistics, or empty if the table has no snapshot
   */
  public Optional<TableSnapshot> loadLatestTableSnapshot(Identifier identifier) {
    FileStoreTable table = getFileTable(identifier);
    Snapshot snapshot = table.snapshotManager().latestSnapshot();
    if (snapshot == null) {
      return Optional.empty();
    }
    List<ManifestEntry> files = table.store().newScan().withSnapshot(snapshot).plan().files();
    long size = 0;
    long creationTime = 0;
    for (ManifestEntry entry : files) {
      size += entry.file().fileSize();
      creationTime = Math.max(creationTime, entry.file().creationTimeEpochMillis());
    }
    return Optional.of(
        new TableSnapshot(snapshot, snapshot.totalRecordCount(), size, files.size(), creationTime));
  }

  /**
   * Loads a snapshot of a table by version: {@code EARLIEST}, {@code LATEST} or a numeric snapshot
   * id or tag name.
   *
   * @param identifier the table identifier
   * @param version the version specifier
   * @return the snapshot
   */
  public Snapshot loadSnapshot(Identifier identifier, String version) {
    FileStoreTable table = getFileTable(identifier);
    SnapshotManager snapshotManager = table.snapshotManager();
    Snapshot snapshot;
    try {
      if (EARLIEST_SNAPSHOT.equalsIgnoreCase(version)) {
        snapshot = snapshotManager.earliestSnapshot();
      } else if (LATEST_SNAPSHOT.equalsIgnoreCase(version)) {
        snapshot = snapshotManager.latestSnapshot();
      } else {
        try {
          snapshot = snapshotManager.tryGetSnapshot(Long.parseLong(version));
        } catch (NumberFormatException e) {
          snapshot = table.tagManager().get(version).map(Tag::trimToSnapshot).orElse(null);
        }
      }
    } catch (FileNotFoundException e) {
      throw snapshotNotExist(identifier, version);
    }
    if (snapshot == null) {
      throw snapshotNotExist(identifier, version);
    }
    return snapshot;
  }

  /**
   * Lists all snapshots of a table in ascending snapshot id order.
   *
   * @param identifier the table identifier
   * @return the snapshots
   */
  public List<Snapshot> listSnapshots(Identifier identifier) {
    FileStoreTable table = getFileTable(identifier);
    try {
      List<Snapshot> snapshots = new ArrayList<>();
      table.snapshotManager().snapshots().forEachRemaining(snapshots::add);
      snapshots.sort(Comparator.comparingLong(Snapshot::id));
      return snapshots;
    } catch (IOException e) {
      throw PaimonCatalogException.failure(
          "Failed to list snapshots of table %s: %s",
          identifier.getFullName(), e.getMessage() == null ? "" : e.getMessage());
    }
  }

  // ------------------------------------------------------------------
  // Branches, tags, rollback and table registration
  // ------------------------------------------------------------------

  /**
   * Lists all branch names of a table.
   *
   * @param identifier the table identifier
   * @return the branch names
   */
  public List<String> listBranches(Identifier identifier) {
    return branchManager(identifier).branches();
  }

  /**
   * Creates a branch, optionally from an existing tag.
   *
   * @param identifier the table identifier
   * @param branch the new branch name
   * @param fromTag the tag to create the branch from, may be null for the latest snapshot
   */
  public void createBranch(Identifier identifier, String branch, String fromTag) {
    BranchManager branchManager = branchManager(identifier);
    try {
      if (fromTag == null) {
        branchManager.createBranch(branch);
      } else {
        branchManager.createBranch(branch, fromTag);
      }
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.BRANCH, branch);
    }
  }

  /**
   * Drops a branch.
   *
   * @param identifier the table identifier
   * @param branch the branch name
   */
  public void dropBranch(Identifier identifier, String branch) {
    try {
      branchManager(identifier).dropBranch(branch);
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.BRANCH, branch);
    }
  }

  /**
   * Renames a branch.
   *
   * @param identifier the table identifier
   * @param branch the current branch name
   * @param toBranch the new branch name
   */
  public void renameBranch(Identifier identifier, String branch, String toBranch) {
    try {
      branchManager(identifier).renameBranch(branch, toBranch);
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.BRANCH, branch);
    }
  }

  /**
   * Fast-forwards a branch so that its head becomes the latest snapshot of the table.
   *
   * @param identifier the table identifier
   * @param branch the branch name
   */
  public void fastForwardBranch(Identifier identifier, String branch) {
    try {
      branchManager(identifier).fastForward(branch);
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.BRANCH, branch);
    }
  }

  /**
   * Lists all tag names of a table in ascending order.
   *
   * @param identifier the table identifier
   * @return the tag names
   */
  public List<String> listTags(Identifier identifier) {
    try {
      List<String> tags = new ArrayList<>(tagManager(identifier).allTagNames());
      tags.sort(Comparator.naturalOrder());
      return tags;
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.TAG, null);
    }
  }

  /**
   * Loads a tag.
   *
   * @param identifier the table identifier
   * @param tagName the tag name
   * @return the tag metadata
   */
  public Tag getTag(Identifier identifier, String tagName) {
    TagManager tagManager = tagManager(identifier);
    Optional<Tag> tag;
    try {
      tag = tagManager.get(tagName);
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.TAG, tagName);
    }
    if (!tag.isPresent()) {
      throw PaimonCatalogException.notFound(
          Resource.TAG,
          tagName,
          "Tag %s of table %s does not exist.",
          tagName,
          identifier.getFullName());
    }
    return tag.get();
  }

  /**
   * Creates a tag on the latest snapshot or a given snapshot id, with an optional retention
   * duration.
   *
   * @param identifier the table identifier
   * @param tagName the tag name
   * @param snapshotId the snapshot id to tag, null for the latest snapshot
   * @param timeRetained the retention duration string, may be null
   */
  public void createTag(
      Identifier identifier, String tagName, Long snapshotId, String timeRetained) {
    FileStoreTable table = getFileTable(identifier);
    java.time.Duration retained = null;
    if (timeRetained != null) {
      try {
        retained = TimeUtils.parseDuration(timeRetained);
      } catch (Exception e) {
        throw PaimonCatalogException.invalid("Invalid timeRetained format: %s", timeRetained);
      }
    }
    try {
      if (snapshotId == null) {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        if (latest == null) {
          throw snapshotNotExist(identifier, "latest");
        }
        if (retained == null) {
          table.createTag(tagName, latest.id());
        } else {
          table.createTag(tagName, latest.id(), retained);
        }
      } else {
        if (retained == null) {
          table.createTag(tagName, snapshotId);
        } else {
          table.createTag(tagName, snapshotId, retained);
        }
      }
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.TAG, tagName);
    }
  }

  /**
   * Deletes a tag.
   *
   * @param identifier the table identifier
   * @param tagName the tag name
   */
  public void deleteTag(Identifier identifier, String tagName) {
    try {
      getFileTable(identifier).deleteTag(tagName);
    } catch (Exception e) {
      throw mapBranchTagException(e, Resource.TAG, tagName);
    }
  }

  /**
   * Rolls a table back to a snapshot. When {@code fromSnapshot} is given it must equal the current
   * latest snapshot id (optimistic-concurrency precondition of the protocol).
   *
   * @param identifier the table identifier
   * @param snapshotId the target snapshot id
   * @param fromSnapshot the expected current latest snapshot id, may be null
   */
  public void rollbackTable(Identifier identifier, long snapshotId, @Nullable Long fromSnapshot) {
    rollbackWithPrecondition(
        identifier,
        fromSnapshot,
        table -> {
          if (snapshotMissing(table, snapshotId)) {
            throw snapshotNotExist(identifier, String.valueOf(snapshotId));
          }
          table.rollbackTo(snapshotId);
        });
  }

  /**
   * Rolls a table back to a tag when the current snapshot matches the optional precondition.
   *
   * @param identifier the table identifier
   * @param tagName the tag name
   * @param fromSnapshot the expected current latest snapshot id, may be null
   */
  public void rollbackTableToTag(
      Identifier identifier, String tagName, @Nullable Long fromSnapshot) {
    rollbackWithPrecondition(
        identifier,
        fromSnapshot,
        table -> {
          if (!table.tagManager().get(tagName).isPresent()) {
            throw PaimonCatalogException.notFound(
                Resource.TAG,
                tagName,
                "Tag %s of table %s does not exist.",
                tagName,
                identifier.getFullName());
          }
          table.rollbackTo(tagName);
        });
  }

  /**
   * Registers an existing table location as a table of this catalog (external table).
   *
   * @param identifier the table identifier
   * @param path the table location path
   */
  public void registerTable(Identifier identifier, String path) {
    try {
      catalog.getDatabase(identifier.getDatabaseName());
    } catch (Catalog.DatabaseNotExistException e) {
      throw databaseNotExist(identifier.getDatabaseName());
    }
    try {
      catalog.registerTable(identifier, path);
    } catch (Catalog.TableAlreadyExistException e) {
      throw PaimonCatalogException.alreadyExists(
          Resource.TABLE,
          identifier.getObjectName(),
          "Table %s already exists.",
          identifier.getFullName());
    }
  }

  /**
   * Closes the wrapped catalog.
   *
   * @throws IOException if the catalog fails to close
   */
  @Override
  public void close() throws IOException {
    try {
      catalog.close();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to close Paimon catalog " + catalogName, e);
    }
  }

  // ------------------------------------------------------------------
  // Private helpers
  // ------------------------------------------------------------------

  private static void validateMutableProperty(String key) {
    if (TABLE_UUID_OPTION.equals(key)) {
      throw new IllegalArgumentException("Table identity property is reserved: " + key);
    }
  }

  private Table getTableOrThrow(Identifier identifier) {
    try {
      catalog.invalidateTable(identifier);
      return catalog.getTable(identifier);
    } catch (Catalog.TableNotExistException e) {
      throw tableNotExist(identifier);
    }
  }

  /**
   * Loads the native file store table without initializing its persistent identity.
   *
   * @param identifier the table identifier
   * @return the native file store table
   */
  public FileStoreTable getFileTable(Identifier identifier) {
    Table table = getTableOrThrow(identifier);
    if (!(table instanceof FileStoreTable)) {
      throw PaimonCatalogException.unsupported(
          "Table %s is not backed by a file store and is not supported by the REST server.",
          identifier.getFullName());
    }
    return (FileStoreTable) table;
  }

  private PaimonCatalogException tableNotExist(Identifier identifier) {
    return PaimonCatalogException.notFound(
        Resource.TABLE,
        identifier.getObjectName(),
        "Table %s does not exist.",
        identifier.getFullName());
  }

  private PaimonCatalogException databaseNotExist(String name) {
    return PaimonCatalogException.notFound(
        Resource.DATABASE, name, "Database %s does not exist.", name);
  }

  private PaimonCatalogException snapshotNotExist(Identifier identifier, String version) {
    return PaimonCatalogException.notFound(
        Resource.SNAPSHOT,
        version,
        "Snapshot %s of table %s does not exist.",
        version,
        identifier.getFullName());
  }

  @Nullable
  private String loadTableUuid(FileStoreTable table) throws IOException {
    String uuid = table.options().get(TABLE_UUID_OPTION);
    if (uuid == null) {
      Path uuidPath = new Path(table.location(), TABLE_UUID_FILE);
      if (table.fileIO().exists(uuidPath)) {
        uuid = table.fileIO().readFileUtf8(uuidPath);
      }
    }
    return uuid;
  }

  private <T> T withTableLock(Identifier identifier, FileStoreTable table, Callable<T> action)
      throws Exception {
    CatalogEnvironment environment = table.catalogEnvironment();
    CatalogLockFactory factory = environment.lockFactory();
    if (factory == null) {
      java.util.concurrent.locks.Lock lock = LOCAL_TABLE_LOCKS.get(table.location().toString());
      lock.lock();
      try {
        return action.call();
      } finally {
        lock.unlock();
      }
    }
    Identifier lockIdentifier =
        new Identifier(
            identifier.getDatabaseName(), identifier.getTableName(), identifier.getBranchName());
    try (Lock lock =
        Lock.fromCatalog(factory.createLock(environment.lockContext()), lockIdentifier)) {
      return lock.runWithLock(action);
    }
  }

  private void rollbackWithPrecondition(
      Identifier identifier, @Nullable Long fromSnapshot, Consumer<FileStoreTable> rollback) {
    FileStoreTable table = getFileTable(identifier);
    try {
      withTableLock(
          identifier,
          table,
          () -> {
            FileStoreTable current = getFileTable(identifier);
            if (fromSnapshot != null) {
              Long latest = current.snapshotManager().latestSnapshotId();
              if (!fromSnapshot.equals(latest)) {
                throw PaimonCatalogException.invalid(
                    "Latest snapshot %s is not %s", latest, fromSnapshot);
              }
            }
            rollback.accept(current);
            return null;
          });
    } catch (PaimonCatalogException e) {
      throw e;
    } catch (Exception e) {
      throw PaimonCatalogException.failure(
          "Failed to roll back table %s: %s", identifier.getFullName(), e.getMessage());
    }
  }

  private BranchManager branchManager(Identifier identifier) {
    return getFileTable(identifier).branchManager();
  }

  private TagManager tagManager(Identifier identifier) {
    return getFileTable(identifier).tagManager();
  }

  private boolean snapshotMissing(FileStoreTable table, long snapshotId) {
    try {
      return table.snapshotManager().tryGetSnapshot(snapshotId) == null;
    } catch (FileNotFoundException e) {
      return true;
    }
  }

  /**
   * Maps Paimon's message-based branch/tag errors to protocol errors: "already exists" becomes 409
   * and "not exist"/"doesn't exist" becomes 404, mirroring the reference REST server.
   */
  private RuntimeException mapBranchTagException(
      Exception e, Resource resourceType, @Nullable String resourceName) {
    String message = e.getMessage() == null ? "" : e.getMessage();
    String lowered = message.toLowerCase(Locale.ROOT);
    if (lowered.contains("already exists")) {
      return PaimonCatalogException.alreadyExists(
          resourceType, resourceName, "Resource %s already exists.", resourceName);
    }
    if (lowered.contains("not exist")
        || lowered.contains("doesn't exist")
        || lowered.contains("does not exist")) {
      return PaimonCatalogException.notFound(
          resourceType, resourceName, "Resource %s does not exist.", resourceName);
    }
    return PaimonCatalogException.failure(
        "Operation failed: %s", message.isEmpty() ? e.getClass().getSimpleName() : message);
  }

  /** A loaded table and the identity associated with its current incarnation. */
  public static final class LoadedTable {
    private final FileStoreTable table;
    private final String id;

    private LoadedTable(FileStoreTable table, String id) {
      this.table = table;
      this.id = id;
    }

    /**
     * @return the native table
     */
    public FileStoreTable table() {
      return table;
    }

    /**
     * @return the persistent table identity
     */
    public String id() {
      return id;
    }
  }

  /** Result of a database alter operation. */
  public static class AlterDatabaseResult {

    private final List<String> removed;
    private final List<String> updated;
    private final List<String> missing;

    /**
     * Creates a result.
     *
     * @param removed the keys that were removed
     * @param updated the keys that were updated
     * @param missing the requested removals that did not exist
     */
    public AlterDatabaseResult(List<String> removed, List<String> updated, List<String> missing) {
      this.removed = removed;
      this.updated = updated;
      this.missing = missing;
    }

    /**
     * Returns the removed keys.
     *
     * @return removed keys
     */
    public List<String> removed() {
      return removed;
    }

    /**
     * Returns the updated keys.
     *
     * @return updated keys
     */
    public List<String> updated() {
      return updated;
    }

    /**
     * Returns the missing keys.
     *
     * @return missing keys
     */
    public List<String> missing() {
      return missing;
    }
  }
}
