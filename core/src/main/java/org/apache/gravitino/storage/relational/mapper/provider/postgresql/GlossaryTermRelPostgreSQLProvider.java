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

package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** PostgreSQL statements for glossary term relations. */
public class GlossaryTermRelPostgreSQLProvider extends GlossaryTermRelBaseSQLProvider {
  @Override
  public String ensureLock(@Param("metalakeId") long metalakeId) {
    return "INSERT INTO glossary_hierarchy_lock (metalake_id) VALUES (#{metalakeId}) "
        + "ON CONFLICT (metalake_id) DO NOTHING";
  }
}
