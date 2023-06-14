/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportNamespaces.java

package com.datastrato.graviton.meta.catalog;

import com.datastrato.graviton.Namespace;
import java.util.Map;

/**
 * The Catalog interface to support namespace operations. If the implemented catalog has namespace
 * semantics, it should implement this interface.
 */
public interface SupportsNamespaces {

  /**
   * List top-level namespaces from the catalog.
   *
   * <p>If an entity such as a table, view exists, its parent namespaces must also exist and must be
   * returned by this discovery method. For example, if table a.b.t exists, this method must return
   * ["a"] in the result array.
   *
   * @return an array of namespace
   * @throws NoSuchNamespaceException if there's no namespace in the catalog
   */
  Namespace[] listNamespaces() throws NoSuchNamespaceException;

  /**
   * List namespaces in a namespace.
   *
   * <p>If an entity such as a table, view exists, its parent namespaces must also exist and must be
   * returned by this discovery method. For example, if table a.b.t exists, this method invoked as
   * listNamespaces(a) must return [a.b] in the result array
   *
   * @param namespace the namespace to list
   * @return an array of namespace
   * @throws NoSuchNamespaceException if the namespace does not exist
   */
  Namespace[] listNamespaces(Namespace namespace) throws NoSuchNamespaceException;

  /**
   * Check if a namespace exists.
   *
   * <p>If an entity such as a table, view exists, its parent namespaces must also exist. For
   * example, if table a.b.t exists, this method invoked as namespaceExists(a) must return true.
   *
   * @param namespace the namespace to check
   * @return true if the namespace exists, false otherwise
   */
  boolean namespaceExists(Namespace namespace);

  /**
   * Create a namespace in the catalog.
   *
   * @param namespace the namespace to create
   * @param metadata the metadata of the namespace
   * @throws NamespaceAlreadyExistsException if the namespace already exists
   */
  void createNamespace(Namespace namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException;

  /**
   * Apply the metadata change to a namespace in the catalog.
   *
   * @param namespace the namespace to alter
   * @param change the metadata change to apply
   * @throws NoSuchNamespaceException if the namespace does not exist
   */
  void alterNamespace(Namespace namespace, NamespaceChange change) throws NoSuchNamespaceException;

  /**
   * Drop a namespace from the catalog with cascade option, recursively dropping all objects within
   * the namespace if cascade is tur.
   *
   * <p>If the catalog implementation does not support this operation, it may throw {@link
   * UnsupportedOperationException}.
   *
   * @param namespace the namespace to drop
   * @param cascade if true, recursively drop all objects within the namespace
   * @return true if the namespace is dropped successfully, false otherwise
   * @throws NonEmptyNamespaceException if the namespace is not empty and cascade is false
   * @throws NoSuchNamespaceException if the namespace does not exist
   */
  boolean dropNamespace(Namespace namespace, boolean cascade)
      throws NonEmptyNamespaceException, NoSuchNamespaceException;
}
