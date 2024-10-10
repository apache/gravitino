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
package org.apache.gravitino.catalog.oceanbase.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.rel.types.Type;

/** Type converter for OceanBase. */
public class OceanBaseTypeConverter extends JdbcTypeConverter {

  // More data types will be added.
  static final String TINYINT = "tinyint";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    throw new GravitinoRuntimeException("Not implemented yet.");
  }

  @Override
  public String fromGravitino(Type type) {
    throw new GravitinoRuntimeException("Not implemented yet.");
  }
}
