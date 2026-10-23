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
package org.apache.gravitino.maintenance.storage.mapper.provider.h2;

import org.apache.gravitino.maintenance.storage.mapper.provider.base.TableMaintenanceActionBaseSQLProvider;

/** H2 SQL provider for {@code table_maintenance_action}. */
public class TableMaintenanceActionH2Provider extends TableMaintenanceActionBaseSQLProvider {

  /** Returns H2 epoch milliseconds without rounding the fractional part across a second. */
  @Override
  protected String currentTimeMillisExpr() {
    // H2 rounds CURRENT_TIMESTAMP(3) up near a second boundary, while UNIX_TIMESTAMP() truncates
    // the original timestamp. Combining them can move the lease clock backwards by a second.
    return "CAST(FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP(9)) * 1000) AS BIGINT)";
  }
}
