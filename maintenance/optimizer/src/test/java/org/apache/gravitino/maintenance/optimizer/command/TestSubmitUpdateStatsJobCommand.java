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
package org.apache.gravitino.maintenance.optimizer.command;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestSubmitUpdateStatsJobCommand {

  @Test
  void redactJobConfigForLogMasksSensitiveUpdaterOptions() {
    Map<String, String> jobConfig = new LinkedHashMap<>();
    jobConfig.put("catalog_name", "rest");
    jobConfig.put(
        "updater_options",
        "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\","
            + "\"auth_type\":\"basic\",\"username\":\"admin\","
            + "\"password\":\"YourSecureGravitinoPassword\","
            + "\"oauth_credential\":\"id:secret\"}");
    jobConfig.put(
        "spark_conf",
        "{\"spark.sql.catalog.rest.rest.auth.type\":\"basic\","
            + "\"spark.sql.catalog.rest.rest.auth.basic.password\":\"spark-secret\"}");

    Map<String, String> redacted = SubmitUpdateStatsJobCommand.redactJobConfigForLog(jobConfig);
    String updaterOptions = redacted.get("updater_options");
    Assertions.assertTrue(updaterOptions.contains("\"password\":\"******\""));
    Assertions.assertTrue(updaterOptions.contains("\"oauth_credential\":\"******\""));
    Assertions.assertTrue(updaterOptions.contains("\"username\":\"admin\""));
    Assertions.assertTrue(
        jobConfig.get("updater_options").contains("\"password\":\"YourSecureGravitinoPassword\""));
    // spark_conf keys are catalog-scoped (e.g. rest.auth.basic.password); only exact sensitive
    // updater-option keys are redacted.
    Assertions.assertTrue(
        redacted
            .get("spark_conf")
            .contains("\"spark.sql.catalog.rest.rest.auth.basic.password\":\"spark-secret\""));
  }
}
