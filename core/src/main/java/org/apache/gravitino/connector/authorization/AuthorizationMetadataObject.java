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
package org.apache.gravitino.connector.authorization;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;

/**
 * AuthorizationMetadataObject interface is used to define the underlying data source metadata
 * object.
 */
public interface AuthorizationMetadataObject {
  /** Underlying data source metadata object type. */
  interface Type {
    MetadataObject.Type metadataObjectType();
  }

  Splitter DOT_SPLITTER = Splitter.on('.');

  Joiner DOT_JOINER = Joiner.on('.');

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
   * identifier of the object, like `catalog`, `catalog.table`, etc.
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
   * Get the parent full name of the given full name.
   *
   * @param names The names of the metadata object
   * @return The parent full name if it exists, otherwise null
   */
  static String getParentFullName(List<String> names) {
    if (names.size() <= 1) {
      return null;
    }

    return DOT_JOINER.join(names.subList(0, names.size() - 1));
  }

  static String getLastName(List<String> names) {
    Preconditions.checkArgument(names.size() > 0, "Cannot get the last name of an empty list");
    return names.get(names.size() - 1);
  }

  /**
   * The type of the object.
   *
   * @return The type of the object.
   */
  Type type();

  default MetadataObject.Type metadataObjectType() {
    return type().metadataObjectType();
  }

  /** Validate different underlying datasource metadata object */
  void validateAuthorizationMetadataObject() throws IllegalArgumentException;
}
