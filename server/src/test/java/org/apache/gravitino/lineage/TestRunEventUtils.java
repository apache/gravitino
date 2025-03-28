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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.server.OpenLineage.RunEvent;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import java.io.File;
import lombok.SneakyThrows;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRunEventUtils {

  @SneakyThrows
  @Test
  void testRunEventUtils() {
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    RunEvent runEvent =
        objectMapper.readValue(
            new File("/Users/fanng/deploy/openlineage/fileset-model.json"), RunEvent.class);
    OpenLineage.RunEvent clientEvent = RunEventUtils.getClientRunEvent(runEvent);
    checkRunEvent(runEvent, clientEvent);
  }

  private void checkRunEvent(RunEvent serverEvent, OpenLineage.RunEvent clientEvent) {
    Assertions.assertEquals(serverEvent.getEventTime(), clientEvent.getEventTime());
    Assertions.assertEquals(
        getClientEventType(serverEvent.getEventType()), clientEvent.getEventType());
    Assertions.assertEquals(serverEvent.getProducer(), clientEvent.getProducer());
    Assertions.assertEquals(serverEvent.getSchemaURL(), clientEvent.getSchemaURL());
  }

  private OpenLineage.RunEvent.EventType getClientEventType(EventType serverType) {
    switch (serverType) {
      case START:
        return OpenLineage.RunEvent.EventType.START;
      case RUNNING:
        return OpenLineage.RunEvent.EventType.RUNNING;
      case COMPLETE:
        return OpenLineage.RunEvent.EventType.COMPLETE;
      case ABORT:
        return OpenLineage.RunEvent.EventType.ABORT;
      case FAIL:
        return OpenLineage.RunEvent.EventType.FAIL;
      case OTHER:
        return OpenLineage.RunEvent.EventType.OTHER;
      default:
    }
    throw new RuntimeException("Should not come here.");
  }
}
