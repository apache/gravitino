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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;

/** Database fixtures for maintenance-action lease tests. */
public final class TableMaintenanceActionTestUtils {

  private TableMaintenanceActionTestUtils() {}

  /**
   * Backdates an unbound claim without waiting for the database clock to advance.
   *
   * @param claim the claim to expire
   * @param timeoutMs the lease timeout
   * @return the expired claim carrying its updated fencing token
   * @throws SQLException if the fixture cannot be updated
   */
  public static OccupancyClaim expireClaim(OccupancyClaim claim, long timeoutMs)
      throws SQLException {
    long expiredAt = claim.createdAt() - 2 * timeoutMs;
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "UPDATE table_maintenance_action SET created_at = ? "
                        + "WHERE id = ? AND job_id IS NULL AND finished_at = 0")) {
      statement.setLong(1, expiredAt);
      statement.setLong(2, claim.id());
      Assertions.assertEquals(1, statement.executeUpdate());
    }
    return new OccupancyClaim(claim.id(), expiredAt);
  }
}
