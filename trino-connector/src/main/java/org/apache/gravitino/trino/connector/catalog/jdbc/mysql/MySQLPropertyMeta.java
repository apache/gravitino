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

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;

public class MySQLPropertyMeta implements HasPropertyMeta {

  static final String TABLE_ENGINE = "engine";
  static final String TABLE_AUTO_INCREMENT_OFFSET = "auto_increment_offset";

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty(TABLE_ENGINE, "The engine that MySQL table uses", "InnoDB", false),
          stringProperty(
              TABLE_AUTO_INCREMENT_OFFSET, "The auto increment offset for the table", null, false));

  public static final String AUTO_INCREMENT = "auto_increment";
  private static final List<PropertyMetadata<?>> COLUMN_PROPERTY_META =
      ImmutableList.of(booleanProperty(AUTO_INCREMENT, "The auto increment column", false, false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return COLUMN_PROPERTY_META;
  }
}
