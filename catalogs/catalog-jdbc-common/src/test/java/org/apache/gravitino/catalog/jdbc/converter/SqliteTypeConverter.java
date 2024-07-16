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
package org.apache.gravitino.catalog.jdbc.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

public class SqliteTypeConverter extends JdbcTypeConverter {

  protected static final Map<Type, String> GRAVITINO_TO_SQLITE_MAPPING = new HashMap<>();

  static {
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.ByteType.get(), "TINYINT");
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.IntegerType.get(), "INTEGER");
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.StringType.get(), "TEXT");
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.BinaryType.get(), "BLOB");
  }

  @Override
  public Type toGravitino(JdbcTypeBean type) {
    return GRAVITINO_TO_SQLITE_MAPPING.entrySet().stream()
        .filter(entry -> StringUtils.equalsIgnoreCase(type.getTypeName(), entry.getValue()))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse(Types.ExternalType.of(type.getTypeName()));
  }

  @Override
  public String fromGravitino(Type type) {
    return GRAVITINO_TO_SQLITE_MAPPING.get(type);
  }

  public Collection<Type> getGravitinoTypes() {
    return GRAVITINO_TO_SQLITE_MAPPING.keySet();
  }
}
