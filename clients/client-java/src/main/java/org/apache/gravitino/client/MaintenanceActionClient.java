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
package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Collections;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.maintenance.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.rest.RESTUtils;

/**
 * Internal TMS cron Spark callbacks for maintenance action sample / finish (§5.2.2).
 *
 * <p>Uses shared wire types from {@code :maintenance:maintenance-common}. <strong>Not a stable
 * public product API.</strong> Obtain instances only via {@link
 * GravitinoClient#maintenanceActionClient()} from built-in maintenance Spark jobs. The constructor
 * is package-private so external callers cannot treat this as a general client surface.
 */
public final class MaintenanceActionClient {

  private final RESTClient restClient;
  private final String metalakeName;

  /**
   * @param client metalake-scoped Gravitino client
   */
  MaintenanceActionClient(GravitinoClient client) {
    Preconditions.checkArgument(client != null, "client must not be null");
    this.restClient = client.restClient();
    this.metalakeName = client.metalakeName();
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
