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

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing a schema in the {@link Catalog}. A Schema is a basic container of
 * relational objects, like tables, views, etc. A Schema can be self-nested, which means it can be
 * schema1.schema2.table.
 *
 * <p>This defines the basic properties of a schema. A catalog implementation with {@link
 * SupportsSchemas} should implement this interface.
 */
@Evolving
public interface Schema extends Auditable {

  /** @return The name of the Schema. */
  String name();

  /** @return The comment of the Schema. Null is returned if the comment is not set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The properties of the Schema. An empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * @return the {@link SupportsTags} if the schema supports tag operations.
   * @throws UnsupportedOperationException if the schema does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Schema does not support tag operations.");
  }

  /**
   * @return the {@link SupportsPolicies} if the schema supports policy operations.
   * @throws UnsupportedOperationException if the schema does not support policy operations.
   */
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("Schema does not support policy operations.");
  }

  /**
   * @return the {@link SupportsRoles} if the schema supports role operations.
   * @throws UnsupportedOperationException if the schema does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Schema does not support role operations.");
  }
}
