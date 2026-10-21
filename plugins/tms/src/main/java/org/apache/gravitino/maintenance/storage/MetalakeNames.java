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
package org.apache.gravitino.maintenance.storage;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Resolves metalake names for TMS entity ids. */
public final class MetalakeNames {

  private MetalakeNames() {}

  /**
   * @param tableId table entity id
   * @return metalake name owning the table, or {@code null} when the table/metalake is missing
   */
  @Nullable
  public static String forTableId(long tableId) {
    List<TablePO> tables =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.listTablePOsByTableIds(Collections.singletonList(tableId)));
    if (tables == null || tables.isEmpty()) {
      return null;
    }
    MetalakePO metalake =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.selectMetalakeMetaById(tables.get(0).getMetalakeId()));
    return metalake == null ? null : metalake.getMetalakeName();
  }
}
