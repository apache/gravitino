/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.common.utils;

import java.util.regex.Pattern;
import org.apache.gravitino.NameIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergIdentifierUtils {
  public static NameIdentifier toGravitinoTableIdentifier(
      String metalake,
      String catalogName,
      TableIdentifier icebergTableIdentifier,
      String separator) {
    String schemaName = String.join(separator, icebergTableIdentifier.namespace().levels());
    return NameIdentifier.of(metalake, catalogName, schemaName, icebergTableIdentifier.name());
  }

  public static NameIdentifier toGravitinoSchemaIdentifier(
      String metalake, String catalogName, Namespace icebergNamespace, String separator) {
    String schemaName = String.join(separator, icebergNamespace.levels());
    return NameIdentifier.of(metalake, catalogName, schemaName);
  }

  /**
   * Converts a logical Gravitino schema name to an Iceberg {@link Namespace} using the given
   * external separator.
   *
   * <p>If the name contains the separator it is split into a multi-level namespace (e.g. {@code
   * "A:B:C"} with {@code ":"} → {@code Namespace.of("A","B","C")}). Flat names are wrapped in a
   * single-level namespace.
   *
   * @param schemaName the logical schema name using the configured external separator
   * @param separator the external namespace separator configured on the server
   * @return the corresponding Iceberg multi-level namespace
   */
  public static Namespace getIcebergNamespaceFromSchemaName(String schemaName, String separator) {
    if (schemaName.contains(separator)) {
      return Namespace.of(schemaName.split(Pattern.quote(separator), -1));
    }
    return Namespace.of(schemaName);
  }

  /**
   * Converts an Iceberg multi-level {@link Namespace} to a schema name string joined by the given
   * separator.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code Namespace.of("A","B","C")} + {@code ":"} → {@code "A:B:C"} (logical)
   * </ul>
   *
   * @param namespace the Iceberg namespace
   * @param separator the separator to join levels with
   * @return the joined schema name, or {@code ""} for an empty namespace
   */
  public static String icebergNamespaceToSchemaName(Namespace namespace, String separator) {
    if (namespace.isEmpty()) {
      return "";
    }
    return String.join(separator, namespace.levels());
  }
}
