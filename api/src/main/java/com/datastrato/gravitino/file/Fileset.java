/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a fileset under a schema {@link Namespace}. A fileset is a virtual
 * concept of the file or directory that is managed by Gravitino. Users can create a fileset object
 * to manage the non-tabular data on the FS-like storage. The typical use case is to manage the
 * training data for AI workloads. The major difference compare to the relational table is that the
 * fileset is schema-free, the main property of the fileset is the storage location of the
 * underlying data.
 *
 * <p>{@link Fileset} defines the basic properties of a fileset object. A catalog implementation
 * with {@link FilesetCatalog} should implement this interface.
 */
@Evolving
public interface Fileset extends Auditable {

  /** An enum representing the type of the fileset object. */
  enum Type {

    /**
     * Fileset is managed by Gravitino. When specified, the data will be deleted when the fileset
     * object is deleted
     */
    MANAGED,

    /**
     * Fileset is not managed by Gravitino. When specified, the data will not be deleted when the
     * fileset object is deleted
     */
    EXTERNAL
  }

  /** @return Name of the fileset object. */
  String name();

  /** @return The comment of the fileset object. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The type of the fileset object. */
  Type type();

  /**
   * Get the storage location of the file or directory path that is managed by this fileset object.
   *
   * <p>The returned storageLocation can either be the one specified when creating the fileset
   * object, or the one specified in the catalog / schema level if the fileset object is created
   * under this catalog / schema.
   *
   * <p>For managed fileset, the storageLocation can be:
   *
   * <p>1) The one specified when creating the fileset object.
   *
   * <p>2) When catalog property "location" is specified but schema property "location" is not
   * specified, then the storageLocation will be "{catalog location}/schemaName/filesetName".
   *
   * <p>3) When catalog property "location" is not specified but schema property "location" is
   * specified, then the storageLocation will be "{schema location}/filesetName".
   *
   * <p>4) When both catalog property "location" and schema property "location" are specified, then
   * the storageLocation will be "{schema location}/filesetName".
   *
   * <p>5) When both catalog property "location" and schema property "location" are not specified,
   * and storageLocation specified when creating the fileset object is null, this situation is
   * illegal.
   *
   * <p>For external fileset, the storageLocation can be:
   *
   * <p>1) The one specified when creating the fileset object.
   *
   * @return The storage location of the fileset object.
   */
  String storageLocation();

  /**
   * @return The properties of the fileset object. Empty map is returned if no properties are set.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
