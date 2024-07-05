/*
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

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.connector.SupportsSchemas;

/**
 * {@code SchemaDispatcher} interface acts as a specialization of the {@link SupportsSchemas}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling schema-related events or actions that are not covered by the standard
 * {@code SupportsSchemas} operations.
 */
public interface SchemaDispatcher extends SupportsSchemas {

  /**
   * Wraps the given {@link SchemaOperationDispatcher} with additional behaviors or operations. This
   * method returns the same {@link SchemaOperationDispatcher} passed as a parameter to allow for
   * chainable method calls. For example: dispatcherA.wrap(dispatcherB).wrap(dispatcherC)
   *
   * @param operationDispatcher The {@link SchemaOperationDispatcher} to wrap. This is the object
   *     that additional behaviors are applied to. It should not be {@code null}.
   * @return The same {@link SchemaOperationDispatcher} instance passed in as the parameter.
   */
  default SchemaDispatcher wrap(SchemaDispatcher operationDispatcher) {
    return operationDispatcher;
  }

  /**
   * Returns a {@link SchemaDispatcher} that delegates the schema operations to the previously
   * wrapped {@link SchemaOperationDispatcher}.
   *
   * @return The {@link SchemaDispatcher} to delegate the schema operations to.
   */
  SchemaDispatcher delegate();
}
