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
 * <p>This is the shared shape used by Trino 440-479, where {@code getComment()} returns a plain
 * {@code String}. Trino 480 changed the method to return {@code Optional<String>}; the version
 * segments from Trino 480 onward replace this class at compile time with a same-named local copy
 * (the shared file is excluded from their source set).
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
    return column.getComment();
  }
}
