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

package org.apache.gravitino.lineage.source.rest;

import io.openlineage.server.OpenLineage.InputDataset;
import io.openlineage.server.OpenLineage.Job;
import io.openlineage.server.OpenLineage.OutputDataset;
import io.openlineage.server.OpenLineage.Run;
import io.openlineage.server.OpenLineage.RunEvent;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestLineageEventValidator {

  private static final ZonedDateTime EVENT_TIME = ZonedDateTime.now(ZoneOffset.UTC);
  private static final URI PRODUCER = URI.create("https://gravitino.apache.org/test");
  private static final URI SCHEMA_URL =
      URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent");

  @Test
  void testAcceptValidEvent() {
    Assertions.assertDoesNotThrow(() -> LineageEventValidator.validate(validEvent()));
  }

  @Test
  void testRejectMissingRequiredFields() {
    assertInvalid("Lineage event cannot be null", null);
    assertInvalid(
        "eventTime is required",
        event(null, PRODUCER, SCHEMA_URL, validRun(), validJob(), null, null));
    assertInvalid(
        "producer is required",
        event(EVENT_TIME, null, SCHEMA_URL, validRun(), validJob(), null, null));
    assertInvalid(
        "schemaURL is required",
        event(EVENT_TIME, PRODUCER, null, validRun(), validJob(), null, null));
    assertInvalid(
        "run is required", event(EVENT_TIME, PRODUCER, SCHEMA_URL, null, validJob(), null, null));
    assertInvalid(
        "run.runId is required",
        event(EVENT_TIME, PRODUCER, SCHEMA_URL, new Run(null, null), validJob(), null, null));
    assertInvalid(
        "job is required", event(EVENT_TIME, PRODUCER, SCHEMA_URL, validRun(), null, null, null));
    assertInvalid(
        "job.namespace is required",
        event(EVENT_TIME, PRODUCER, SCHEMA_URL, validRun(), new Job(" ", "job", null), null, null));
    assertInvalid(
        "job.name is required",
        event(
            EVENT_TIME,
            PRODUCER,
            SCHEMA_URL,
            validRun(),
            new Job("namespace", " ", null),
            null,
            null));
  }

  @Test
  void testRejectInvalidDatasets() {
    assertInvalid(
        "inputs[0] cannot be null",
        event(
            EVENT_TIME,
            PRODUCER,
            SCHEMA_URL,
            validRun(),
            validJob(),
            Arrays.asList((InputDataset) null),
            null));
    assertInvalid(
        "inputs[0].namespace is required",
        event(
            EVENT_TIME,
            PRODUCER,
            SCHEMA_URL,
            validRun(),
            validJob(),
            List.of(new InputDataset(" ", "catalog.schema.table", null, null)),
            null));
    assertInvalid(
        "outputs[0].name is required",
        event(
            EVENT_TIME,
            PRODUCER,
            SCHEMA_URL,
            validRun(),
            validJob(),
            null,
            List.of(new OutputDataset("metalake", " ", null, null))));
  }

  private static RunEvent validEvent() {
    return event(
        EVENT_TIME,
        PRODUCER,
        SCHEMA_URL,
        validRun(),
        validJob(),
        List.of(new InputDataset("metalake", "catalog.schema.table", null, null)),
        List.of(new OutputDataset("metalake", "catalog.schema.table", null, null)));
  }

  private static RunEvent event(
      ZonedDateTime eventTime,
      URI producer,
      URI schemaURL,
      Run run,
      Job job,
      List<InputDataset> inputs,
      List<OutputDataset> outputs) {
    return new RunEvent(eventTime, producer, schemaURL, EventType.START, run, job, inputs, outputs);
  }

  private static Run validRun() {
    return new Run(UUID.randomUUID(), null);
  }

  private static Job validJob() {
    return new Job("namespace", "job", null);
  }

  private static void assertInvalid(String message, RunEvent event) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> LineageEventValidator.validate(event));
    Assertions.assertEquals(message, exception.getMessage());
  }
}
