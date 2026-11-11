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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestTableMaintenanceActionH2Provider {

  private static final class ExposedH2Provider extends TableMaintenanceActionH2Provider {
    String clockExpression() {
      return currentTimeMillisExpr();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void testClockDoesNotMoveBackwardsAtRoundingBoundary(String timeZone) throws Exception {
    // Replace clock reads with one fixed input, retaining each requested timestamp precision.
    // In particular, the old expression used unrounded seconds and rounded milliseconds.
    String clockExpression =
        new ExposedH2Provider()
            .clockExpression()
            .replace("UNIX_TIMESTAMP()", "UNIX_TIMESTAMP(clock_value)")
            .replaceAll(
                "CURRENT_TIMESTAMP\\((\\d+)\\)",
                "CAST(clock_value AS TIMESTAMP($1) WITH TIME ZONE)");
    String sql =
        "SELECT "
            + clockExpression
            + " FROM (VALUES (CAST(? AS TIMESTAMP(9) WITH TIME ZONE))) AS clock(clock_value)";

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:tms_clock;MODE=MYSQL");
        Statement statement = connection.createStatement();
        PreparedStatement query = connection.prepareStatement(sql)) {
      statement.execute("SET TIME ZONE '" + timeZone + "'");
      for (String timestamp :
          List.of(
              "2026-10-19T00:00:00.999499Z",
              "2026-10-19T00:00:00.999500Z",
              "2026-10-19T00:00:01Z")) {
        query.setString(1, timestamp);
        try (ResultSet result = query.executeQuery()) {
          Assertions.assertTrue(result.next());
          Assertions.assertEquals(Instant.parse(timestamp).toEpochMilli(), result.getLong(1));
        }
      }
    }
  }
}
