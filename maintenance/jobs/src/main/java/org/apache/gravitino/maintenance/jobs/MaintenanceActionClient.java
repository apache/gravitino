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
package org.apache.gravitino.maintenance.jobs;

import com.google.common.base.Preconditions;
import java.util.Collections;
import org.apache.gravitino.client.ErrorHandlers;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.RESTClient;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.maintenance.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.rest.RESTUtils;

/**
 * TMS cron Spark callbacks for maintenance action sample / finish (§5.2.2).
 *
 * <p>Lives in {@code :maintenance:jobs} with shared wire types from {@code
 * :maintenance:maintenance-common}. Not part of the public {@code client-java} API.
 */
public final class MaintenanceActionClient {

  private final RESTClient restClient;
  private final String metalakeName;

  /**
   * @param client metalake-scoped Gravitino client
   * @param metalakeName metalake used in the callback path
   */
  public MaintenanceActionClient(GravitinoClient client, String metalakeName) {
    Preconditions.checkArgument(client != null, "client must not be null");
    Preconditions.checkArgument(
        metalakeName != null && !metalakeName.trim().isEmpty(), "metalakeName must not be blank");
    this.restClient = client.restClient();
    this.metalakeName = metalakeName.trim();
  }

  /**
   * Reports an in-job S0 sample and returns the continue / stop gate outcome.
   *
   * @param jobId Gravitino job run id
   * @param request sample payload
   * @return gate outcome
   */
  public MaintenanceActionSampleResponse reportSample(
      long jobId, MaintenanceActionSampleRequest request) {
    Preconditions.checkArgument(request != null, "request must not be null");
    request.validate();
    MaintenanceActionSampleResponse response =
        restClient.post(
            String.format(
                "api/metalakes/%s/maintenance/actions/%d/sample",
                RESTUtils.encodeString(metalakeName), jobId),
            request,
            MaintenanceActionSampleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.restErrorHandler());
    response.validate();
    return response;
  }

  /**
   * Finishes a TMS cron maintenance action, optionally persisting S1 metrics.
   *
   * @param jobId Gravitino job run id
   * @param request finish payload
   */
  public void finish(long jobId, MaintenanceActionFinishRequest request) {
    Preconditions.checkArgument(request != null, "request must not be null");
    request.validate();
    BaseResponse response =
        restClient.post(
            String.format(
                "api/metalakes/%s/maintenance/actions/%d/finish",
                RESTUtils.encodeString(metalakeName), jobId),
            request,
            BaseResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.restErrorHandler());
    response.validate();
  }
}
