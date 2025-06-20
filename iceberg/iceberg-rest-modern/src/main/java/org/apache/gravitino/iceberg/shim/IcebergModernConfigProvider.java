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

package org.apache.gravitino.iceberg.shim;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;

public class IcebergModernConfigProvider implements IcebergRESTConfigProvider {

  @Override
  public ConfigResponse getConfig(String warehouse) {
    return ConfigResponse.builder().withEndpoints(getEndpoints(warehouse)).build();
  }

  private List<Endpoint> getEndpoints(String warehouse) {
    System.out.println("warehouse: " + warehouse);
    return Arrays.asList(
        Endpoint.V1_CREATE_TABLE, Endpoint.V1_LIST_TABLES, Endpoint.V1_DELETE_TABLE);
  }
}
