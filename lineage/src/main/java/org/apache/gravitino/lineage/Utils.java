/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineage;
import io.openlineage.server.OpenLineage.Job;
import io.openlineage.server.OpenLineage.Run;
import io.openlineage.server.OpenLineage.RunEvent;
import org.apache.gravitino.server.web.ObjectMapperProvider;

public class Utils {
  private Utils() {}

  public static String getRunID(RunEvent event) {
    Run run = event.getRun();
    return run == null ? "Unknown" : run.getRunId().toString();
  }

  public static String getJobName(RunEvent event) {
    Job job = event.getJob();
    return job == null ? "Unknown" : job.getName();
  }

  public static OpenLineage.RunEvent getClientRunEvent(RunEvent event)
      throws JsonProcessingException {
    String value = ObjectMapperProvider.objectMapper().writeValueAsString(event);
    return ObjectMapperProvider.objectMapper()
        .readValue(value, new TypeReference<OpenLineage.RunEvent>() {});
  }
}
