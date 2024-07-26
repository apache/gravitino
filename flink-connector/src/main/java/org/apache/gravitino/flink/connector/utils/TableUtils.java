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

package org.apache.gravitino.flink.connector.utils;

import org.apache.gravitino.rel.TableChange;

public class TableUtils {
  private TableUtils() {}

  public static TableChange.ColumnPosition toGravitinoColumnPosition(
      org.apache.flink.table.catalog.TableChange.ColumnPosition columnPosition) {
    if (columnPosition == null) {
      return null;
    }

    if (columnPosition instanceof org.apache.flink.table.catalog.TableChange.First) {
      return TableChange.ColumnPosition.first();
    } else if (columnPosition instanceof org.apache.flink.table.catalog.TableChange.After) {
      org.apache.flink.table.catalog.TableChange.After after =
          (org.apache.flink.table.catalog.TableChange.After) columnPosition;
      return TableChange.ColumnPosition.after(after.column());
    } else {
      throw new IllegalArgumentException(
          String.format("Not support column position : %s", columnPosition.getClass()));
    }
  }
}
