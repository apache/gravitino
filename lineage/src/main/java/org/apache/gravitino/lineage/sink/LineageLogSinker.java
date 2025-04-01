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

package org.apache.gravitino.lineage.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.EnumFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.openlineage.server.OpenLineage.RunEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageLogSinker implements LineageSink {
  private static final Logger LOG = LoggerFactory.getLogger(LineageLogSinker.class);
  private ObjectMapper objectMapper =
      JsonMapper.builder()
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .configure(EnumFeature.WRITE_ENUMS_TO_LOWERCASE, true)
          .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
          .build()
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
          .registerModule(new JavaTimeModule())
          .registerModule(new Jdk8Module());

  public LineageLogSinker() {}

  @Override
  public void sink(RunEvent event) {
    try {
      LOG.info("Open lineage event:{}", objectMapper.writeValueAsString(event));
    } catch (Exception e) {
      LOG.warn("Write open lineage event failed,", e);
    }
  }
}
