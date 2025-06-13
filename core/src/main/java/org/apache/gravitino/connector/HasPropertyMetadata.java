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
package org.apache.gravitino.connector;

import org.apache.gravitino.annotation.Evolving;

/** This interface represents entities that have property metadata. */
@Evolving
public interface HasPropertyMetadata {

  /**
   * Returns the table property metadata.
   *
   * @return The table property metadata.
   * @throws UnsupportedOperationException if the entity does not support table properties.
   */
  PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the catalog property metadata.
   *
   * @return The catalog property metadata.
   * @throws UnsupportedOperationException if the entity does not support catalog properties.
   */
  PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the schema property metadata.
   *
   * @return The schema property metadata.
   * @throws UnsupportedOperationException if the entity does not support schema properties.
   */
  PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the fileset property metadata.
   *
   * @return The fileset property metadata.
   * @throws UnsupportedOperationException if the entity does not support fileset properties.
   */
  PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the topic property metadata.
   *
   * @return The topic property metadata.
   * @throws UnsupportedOperationException if the entity does not support topic properties.
   */
  PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the model property metadata.
   *
   * @return The model property metadata.
   * @throws UnsupportedOperationException if the entity does not support model properties.
   */
  PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the model version property metadata.
   *
   * @return The model version property metadata.
   * @throws UnsupportedOperationException if the entity does not support model version properties.
   */
  PropertiesMetadata modelVersionPropertiesMetadata() throws UnsupportedOperationException;
}
