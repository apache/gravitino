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
package org.apache.gravitino;

import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.tag.SupportsTags;

/**
 * The interface of a catalog. The catalog is the second level entity in the Gravitino system,
 * containing a set of tables. The server side should use the other one with the same name in the
 * core module.
 */
@Evolving
public interface Catalog extends Auditable {

  /** The type of the catalog. */
  enum Type {
    /** Catalog Type for Relational Data Structure, like db.table, catalog.db.table. */
    RELATIONAL(false),

    /** Catalog Type for Fileset System (including HDFS, S3, etc.), like path/to/file */
    FILESET(true),

    /** Catalog Type for Message Queue, like Kafka://topic */
    MESSAGING(false),

    /** Catalog Type for ML model */
    MODEL(true),

    /** Catalog Type for test only. */
    UNSUPPORTED(false);

    private final boolean supportsManagedCatalog;

    Type(boolean supportsManagedCatalog) {
      this.supportsManagedCatalog = supportsManagedCatalog;
    }

    /**
     * A flag to indicate whether the catalog type supports managed catalog. Managed catalog is a
     * concept in Gravitino, for the details of managed catalog, please refer to the class comment
     * of {@link CatalogProvider}. If the catalog type supports managed catalog, users can create
     * managed catalog of this type without specifying the provider, Gravitino will use the type as
     * the provider to create the managed catalog. If the catalog type does not support managed
     * catalog, users need to specify the provider when creating the catalog.
     *
     * <p>Currently, the model and fileset catalogs support managed catalog.
     *
     * @return Whether the catalog type supports managed catalog. Returns true if the catalog type
     *     supports managed catalog.
     */
    public boolean supportsManagedCatalog() {
      return supportsManagedCatalog;
    }

    /**
     * Convert the string (case-insensitive) to the catalog type.
     *
     * @param type The string to convert
     * @return The catalog type
     */
    public static Type fromString(String type) {
      switch (type.toLowerCase(Locale.ROOT)) {
        case "relational":
          return RELATIONAL;
        case "fileset":
          return FILESET;
        case "messaging":
          return MESSAGING;
        case "model":
          return MODEL;
        default:
          throw new IllegalArgumentException("Unknown catalog type: " + type);
      }
    }
  }

  /** The cloud that the catalog is running on. Used by the catalog property `cloud.name`. */
  enum CloudName {
    /** Amazon Web Services */
    AWS,

    /** Microsoft Azure */
    AZURE,

    /** Google Cloud Platform */
    GCP,

    /** Not running on cloud */
    ON_PREMISE,

    /** Other cloud providers */
    OTHER
  }

  /**
   * A reserved property to specify the package location of the catalog. The "package" is a string
   * of path to the folder where all the catalog related dependencies is located. The dependencies
   * under the "package" will be loaded by Gravitino to create the catalog.
   *
   * <p>The property "package" is not needed if the catalog is a built-in one, Gravitino will search
   * the proper location using "provider" to load the dependencies. Only when the folder is in
   * different location, the "package" property is needed.
   */
  String PROPERTY_PACKAGE = "package";

  /** The property indicates the catalog is in use. */
  String PROPERTY_IN_USE = "in-use";

  /**
   * The property to specify the cloud that the catalog is running on. The value should be one of
   * the {@link CloudName}.
   */
  String CLOUD_NAME = "cloud.name";

  /**
   * The property to specify the region code of the cloud that the catalog is running on. The value
   * should be the region code of the cloud provider.
   */
  String CLOUD_REGION_CODE = "cloud.region-code";

  /**
   * This variable is used as a key in properties of catalogs to use authorization provider in
   * Gravitino.
   */
  String AUTHORIZATION_PROVIDER = "authorization-provider";

  /** @return The name of the catalog. */
  String name();

  /** @return The type of the catalog. */
  Type type();

  /** @return The provider of the catalog. */
  String provider();

  /**
   * The comment of the catalog. Note. this method will return null if the comment is not set for
   * this catalog.
   *
   * @return The comment of the catalog.
   */
  String comment();

  /**
   * The properties of the catalog. Note, this method will return null if the properties are not
   * set.
   *
   * @return The properties of the catalog.
   */
  Map<String, String> properties();

  /**
   * Return the {@link SupportsSchemas} if the catalog supports schema operations.
   *
   * @return The {@link SupportsSchemas} if the catalog supports schema operations.
   * @throws UnsupportedOperationException if the catalog does not support schema operations.
   */
  default SupportsSchemas asSchemas() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support schema operations");
  }

  /**
   * @return the {@link TableCatalog} if the catalog supports table operations.
   * @throws UnsupportedOperationException if the catalog does not support table operations.
   */
  default TableCatalog asTableCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support table operations");
  }

  /**
   * @return the {@link FilesetCatalog} if the catalog supports fileset operations.
   * @throws UnsupportedOperationException if the catalog does not support fileset operations.
   */
  default FilesetCatalog asFilesetCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support fileset operations");
  }

  /**
   * @return the {@link TopicCatalog} if the catalog supports topic operations.
   * @throws UnsupportedOperationException if the catalog does not support topic operations.
   */
  default TopicCatalog asTopicCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support topic operations");
  }

  /**
   * @return the {@link ModelCatalog} if the catalog supports model operations.
   * @throws UnsupportedOperationException if the catalog does not support model operations.
   */
  default ModelCatalog asModelCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support model operations");
  }

  /**
   * @return the {@link SupportsTags} if the catalog supports tag operations.
   * @throws UnsupportedOperationException if the catalog does not support tag operations.
   */
  default SupportsTags supportsTags() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support tag operations");
  }

  /**
   * @return the {@link SupportsPolicies} if the catalog supports policy operations.
   * @throws UnsupportedOperationException if the catalog does not support policy operations.
   */
  default SupportsPolicies supportsPolicies() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support policy operations");
  }

  /**
   * @return the {@link SupportsRoles} if the catalog supports role operations.
   * @throws UnsupportedOperationException if the catalog does not support role operations.
   */
  default SupportsRoles supportsRoles() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support role operations");
  }

  /**
   * @return the {@link SupportsCredentials} if the catalog supports credential operations.
   * @throws UnsupportedOperationException if the catalog does not support credential operations.
   */
  default SupportsCredentials supportsCredentials() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support credential operations");
  }
}
