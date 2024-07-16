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

import org.apache.gravitino.rel.types.Type;

/**
 * The interface for converting data types between Apache Gravitino and catalogs. In most cases, the
 * ToType and FromType are the same. But in some cases, such as converting between Gravitino and
 * JDBC types, the ToType is String and the FromType is JdbcTypeBean.
 *
 * @param <ToType> The Gravitino type will be converted to.
 * @param <FromType> The type will be converted to Gravitino type.
 */
public interface DataTypeConverter<ToType, FromType> {
  /**
   * Convert the Gravitino type to the catalog type.
   *
   * @param type The Gravitino type.
   * @return The catalog type.
   */
  ToType fromGravitino(Type type);

  /**
   * Convert the catalog type to the Gravitino type.
   *
   * @param type The catalog type.
   * @return The Gravitino type.
   */
  Type toGravitino(FromType type);
}
