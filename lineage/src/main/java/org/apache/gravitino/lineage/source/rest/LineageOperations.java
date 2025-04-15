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

package org.apache.gravitino.lineage.source.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import io.openlineage.server.OpenLineage;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lineage.LineageDispatcher;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/lineage")
public class LineageOperations {

  private static final Logger LOG = LoggerFactory.getLogger(LineageOperations.class);
  private LineageDispatcher lineageDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public LineageOperations(LineageDispatcher lineageDispatcher) {
    this.lineageDispatcher = lineageDispatcher;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "post-lineage." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "post-lineage", absolute = true)
  public Response postLineage(OpenLineage.RunEvent event) {
    LOG.info(
        "Open lineage event, run id:{}, job name:{}",
        org.apache.gravitino.lineage.Utils.getRunID(event),
        org.apache.gravitino.lineage.Utils.getJobName(event));

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (lineageDispatcher.dispatchLineageEvent(event)) {
              return Utils.created();
            } else {
              return Utils.tooManyRequests();
            }
          });
    } catch (Exception e) {
      LOG.warn("Process lineage failed,", e);
      return Utils.internalError(e.getMessage(), e);
    }
  }
}
