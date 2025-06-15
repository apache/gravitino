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

package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.gravitino.catalog.mysql.MysqlConstants;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.shaded.com.google.common.collect.ImmutableMap;

/**
 * Property converter for MySQL table properties. Handles the conversion of property keys between
 * Trino and Gravitino formats for MySQL tables.
 */
public class MySQLTablePropertyConverter extends PropertyConverter {
  // TODO (yuqi) add more properties
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(MySQLPropertyMeta.TABLE_ENGINE, MysqlConstants.GRAVITINO_ENGINE_KEY)
              .put(
                  MySQLPropertyMeta.TABLE_AUTO_INCREMENT_OFFSET,
                  MysqlConstants.GRAVITINO_AUTO_INCREMENT_OFFSET_KEY)
              .build());

  @Override
  public Map<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }
}
