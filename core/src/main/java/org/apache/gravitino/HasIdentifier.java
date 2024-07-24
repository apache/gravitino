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

/** This interface represents entities that have identifiers. */
public interface HasIdentifier {

  /**
   * Get the name of the entity.
   *
   * @return The name of the entity.
   */
  String name();

  /**
   * Get the unique id of the entity.
   *
   * @return The unique id of the entity.
   */
  Long id();

  /**
   * Get the namespace of the entity.
   *
   * @return The namespace of the entity.
   */
  default Namespace namespace() {
    return Namespace.empty();
  }

  /**
   * Get the name identifier of the entity.
   *
   * @return The name identifier of the entity.
   */
  default NameIdentifier nameIdentifier() {
    return NameIdentifier.of(namespace(), name());
  }
}
