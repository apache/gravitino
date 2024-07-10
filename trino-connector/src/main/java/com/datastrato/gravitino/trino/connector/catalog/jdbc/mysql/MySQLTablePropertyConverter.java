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

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.TABLE_AUTO_INCREMENT_OFFSET;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.TABLE_ENGINE;

import com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class MySQLTablePropertyConverter extends PropertyConverter {
  // TODO (yuqi) add more properties
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(TABLE_ENGINE, MysqlTablePropertiesMetadata.GRAVITINO_ENGINE_KEY)
              .put(
                  TABLE_AUTO_INCREMENT_OFFSET,
                  MysqlTablePropertiesMetadata.GRAVITINO_AUTO_INCREMENT_OFFSET_KEY)
              .build());

  @Override
  public Map<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }
}
