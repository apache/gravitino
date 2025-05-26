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
package org.apache.gravitino.server.web.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/audit")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AuditOperations {

  private static final Logger LOG = LoggerFactory.getLogger(AuditOperations.class);

  private static final String AUDIT_FILE = "gravitino_audit.log";

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  public Response getAuditLog() {
    LOG.info("Received request to get audit log");

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (gravitinoHome == null) {
      LOG.error("GRAVITINO_HOME environment variable is not set");
      return Utils.internalError("GRAVITINO_HOME environment variable is not set");
    }

    String auditFilePath = gravitinoHome + "/logs/" + AUDIT_FILE;
    File auditFile = new File(auditFilePath);
    if (!auditFile.exists()) {
      LOG.error("Audit log file does not exist: {}", auditFilePath);
      return Utils.internalError("Audit log file does not exist");
    }

    try (FileInputStream fis = new FileInputStream(auditFile);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8))) {
      List<String> audits = Lists.newArrayList();
      String line;

      while ((line = reader.readLine()) != null) {
        audits.add(line);
      }

      return Response.ok(new AuditLogResponse(audits)).build();
    } catch (IOException e) {
      LOG.error("Error reading audit log file: {}", e.getMessage());
      return Utils.internalError("Error reading audit log file");
    }
  }

  @Getter
  @EqualsAndHashCode(callSuper = true)
  @ToString
  public static class AuditLogResponse extends BaseResponse {

    @JsonProperty("audits")
    private final List<String> auditLogs;

    public AuditLogResponse(List<String> auditLogs) {
      super(0);
      this.auditLogs = auditLogs;
    }

    public AuditLogResponse() {
      super();
      this.auditLogs = null;
    }
  }
}
