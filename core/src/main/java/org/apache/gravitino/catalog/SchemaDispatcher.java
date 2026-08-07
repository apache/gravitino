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

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretReference;

/**
 * {@code SchemaDispatcher} interface acts as a specialization of the {@link SupportsSchemas}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling schema-related events or actions that are not covered by the standard
 * {@code SupportsSchemas} operations.
 */
public interface SchemaDispatcher extends SupportsSchemas {

  /**
   * Create a schema in the catalog with optional secret maps.
   *
   * <p>The default implementation rejects create-time secrets. Implementations that support secrets
   * must override this method.
   *
   * @param ident The name identifier of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @param secretBindings optional property key → binding ({@code provider} + {@code plaintext})
   *     for write-through
   * @param secretReferences optional property key → secret locator ({@code provider} plus
   *     provider-specific attributes).
   * @return The created schema.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws SchemaAlreadyExistsException If the schema already exists.
   * @throws UnsupportedOperationException if create-time secrets are not supported
   */
  default Schema createSchema(
      NameIdentifier ident,
      String comment,
      Map<String, String> properties,
      Map<String, SecretBinding> secretBindings,
      Map<String, SecretReference> secretReferences)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
