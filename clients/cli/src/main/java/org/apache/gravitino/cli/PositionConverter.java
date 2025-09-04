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

package org.apache.gravitino.cli;

import org.apache.gravitino.rel.TableChange;

/** Converts a position string to the appropriate position. */
public class PositionConverter {

  /**
   * Converts a position string to the appropriate position.
   *
   * @param position The string representing the position i.e. "first" or a column name.
   * @return An instance of the appropriate position.
   * @throws IllegalArgumentException if the type name is not recognized.
   */
  public static TableChange.ColumnPosition convert(String position) {

    if (position == null || position.isEmpty()) {
      return TableChange.ColumnPosition.defaultPos();
    } else if (position.equalsIgnoreCase("first")) {
      return TableChange.ColumnPosition.first();
    } else {
      return TableChange.ColumnPosition.after(position);
    }
  }
}
