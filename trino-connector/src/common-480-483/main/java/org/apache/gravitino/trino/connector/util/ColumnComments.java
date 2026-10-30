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
package org.apache.gravitino.trino.connector.util;

import io.trino.spi.connector.ColumnMetadata;

/**
 * Reads the comment of a Trino {@link ColumnMetadata}.
 *
 * <p>Trino 480 changed {@code getComment()} to return {@code Optional<String>}.
 */
public final class ColumnComments {

  private ColumnComments() {}

  /**
   * Returns the comment of the given column metadata.
   *
   * @param column the Trino column metadata to read
   * @return the column comment, or {@code null} if there is none
   */
  public static String read(ColumnMetadata column) {
    return column.getComment().orElse(null);
  }
}
