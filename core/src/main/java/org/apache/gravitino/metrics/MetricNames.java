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

package org.apache.gravitino.metrics;

public class MetricNames {
  public static final String HTTP_PROCESS_DURATION = "http-request-duration-seconds";
  public static final String SERVER_IDLE_THREAD_NUM = "http-server.idle-thread.num";
  public static final String DATASOURCE_ACTIVE_CONNECTIONS = "datasource.active-connections";
  public static final String DATASOURCE_IDLE_CONNECTIONS = "datasource.idle-connections";
  public static final String DATASOURCE_MAX_CONNECTIONS = "datasource.max-connections";

  private MetricNames() {}
}
