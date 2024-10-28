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

package org.apache.gravitino.authorization.ranger;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Unstable;

/**
 * The Ranger MetadataObject is the basic unit of the Gravitino system. It represents the Apache
 * Ranger metadata object in the Apache Gravitino system. The object can be a catalog, schema,
 * table, column, etc.
 */
@Unstable
public interface RangerMetadataObject {
  /**
   * The type of object in the Ranger system. Every type will map one kind of the entity of the
   * Gravitino type system.
   */
  enum Type {
    /** A schema is a sub collection of the catalog. The schema can contain tables, columns, etc. */
    SCHEMA(MetadataObject.Type.SCHEMA),
    /** A table is mapped the table of relational data sources like Apache Hive, MySQL, etc. */
    TABLE(MetadataObject.Type.TABLE),
    /** A column is a sub-collection of the table that represents a group of same type data. */
    COLUMN(MetadataObject.Type.COLUMN);

    private final MetadataObject.Type metadataType;

    Type(MetadataObject.Type type) {
      this.metadataType = type;
    }

    public MetadataObject.Type getMetadataType() {
      return metadataType;
    }

    public static Type fromMetadataType(MetadataObject.Type metadataType) {
      for (Type type : Type.values()) {
        if (type.getMetadataType() == metadataType) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "No matching RangerMetadataObject.Type for " + metadataType);
    }
  }

  /**
   * The parent full name of the object. If the object doesn't have parent, this method will return
   * null.
   *
   * @return The parent full name of the object.
   */
  @Nullable
  String parent();

  /**
   * The name of the object.
   *
   * @return The name of the object.
   */
  String name();

  /**
   * The all name list of the object.
   *
   * @return The name list of the object.
   */
  List<String> names();

  /**
   * The full name of the object. Full name will be separated by "." to represent a string
   * identifier of the object, like catalog, catalog.table, etc.
   *
   * @return The name of the object.
   */
  default String fullName() {
    if (parent() == null) {
      return name();
    } else {
      return parent() + "." + name();
    }
  }

  /**
   * The type of the object.
   *
   * @return The type of the object.
   */
  Type type();
}
