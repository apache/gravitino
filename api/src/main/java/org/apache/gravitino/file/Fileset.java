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
package org.apache.gravitino.file;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing a fileset under a schema {@link Namespace}. A fileset is a virtual
 * concept of the file or directory that is managed by Apache Gravitino. Users can create a fileset
 * object to manage the non-tabular data on the FS-like storage. The typical use case is to manage
 * the training data for AI workloads. The major difference compare to the relational table is that
 * the fileset is schema-free, the main property of the fileset is the storage location of the
 * underlying data.
 *
 * <p>{@link Fileset} defines the basic properties of a fileset object. A catalog implementation
 * with {@link FilesetCatalog} should implement this interface.
 */
@Evolving
public interface Fileset extends Auditable {

  /** The prefix of fileset placeholder property */
  String LOCATION_PLACEHOLDER_PREFIX = "placeholder-";

  /**
   * The reserved property name for the catalog name placeholder, when creating a fileset, all
   * placeholders as {{catalog}} will be replaced by the catalog name
   */
  String RESERVED_CATALOG_PLACEHOLDER = "placeholder-catalog";

  /**
   * The reserved property name for the schema name placeholder, when creating a fileset, all
   * placeholders as {{schema}} will be replaced by the schema name
   */
  String RESERVED_SCHEMA_PLACEHOLDER = "placeholder-schema";

  /**
   * The reserved property name for the fileset name placeholder, when creating a fileset, all
   * placeholders as {{fileset}} will be replaced by the fileset name
   */
  String RESERVED_FILESET_PLACEHOLDER = "placeholder-fileset";

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
   * <p>The storageLocation in each level can contain placeholders, format as {{name}}, which will
   * be replaced by the corresponding fileset property value when the fileset object is created. The
   * placeholder property in the fileset object is formed as "placeholder-{{name}}". For example, if
   * the storageLocation is "file:///path/{{schema}}-{{fileset}}-{{version}}", and the fileset
   * object "catalog1.schema1.fileset1" has the property "placeholder-version" set to "v1", then the
   * storageLocation will be "file:///path/schema1-fileset1-v1".
   *
   * <p>For managed fileset, the storageLocation can be:
   *
   * <p>1) The one specified when creating the fileset object, and the placeholders in the
   * storageLocation will be replaced by the placeholder value specified in the fileset properties.
   *
   * <p>2) When catalog property "location" is specified but schema property "location" is not
   * specified, then the storageLocation will be:
   *
   * <p>a. "{catalog location}/schemaName/filesetName" if {catalog location} does not contain any
   * placeholder.
   *
   * <p>b. "{catalog location}" - placeholders in the {catalog location} will be replaced by the
   * placeholder value specified in the fileset properties.
   *
   * <p>3) When catalog property "location" is not specified but schema property "location" is
   * specified, then the storageLocation will be:
   *
   * <p>a. "{schema location}/filesetName" if {schema location} does not contain any placeholder.
   *
   * <p>b. "{schema location}" - placeholders in the {schema location} will be replaced by the
   * placeholder value specified in the fileset properties.
   *
   * <p>4) When both catalog property "location" and schema property "location" are specified, then
   * the storageLocation will be:
   *
   * <p>a. "{schema location}/filesetName" if {schema location} does not contain any placeholder.
   *
   * <p>b. "{schema location}" - placeholders in the {schema location} will be replaced by the
   *
   * <p>5) When both catalog property "location" and schema property "location" are not specified,
   * and storageLocation specified when creating the fileset object is null, this situation is
   * illegal.
   *
   * <p>For external fileset, the storageLocation can be:
   *
   * <p>1) The one specified when creating the fileset object, and the placeholders in the
   * storageLocation will be replaced by the placeholder value specified in the fileset properties.
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

  /**
   * @return The {@link SupportsTags} if the fileset supports tag operations.
   * @throws UnsupportedOperationException If the fileset does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Fileset does not support tag operations.");
  }

  /**
   * @return The {@link SupportsRoles} if the fileset supports role operations.
   * @throws UnsupportedOperationException If the fileset does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Fileset does not support role operations.");
  }

  /**
   * @return The {@link SupportsCredentials} if the fileset supports credential operations.
   * @throws UnsupportedOperationException If the fileset does not support credential operations.
   */
  default SupportsCredentials supportsCredentials() {
    throw new UnsupportedOperationException("Fileset does not support credential operations.");
  }
}
