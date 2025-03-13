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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.gravitino.authorization.common.ErrorMessages;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;

/** Implementation of a Ranger authorization in Gravitino. */
public class RangerAuthorization extends BaseAuthorization<RangerAuthorization> {
  @Override
  public String shortName() {
    return "ranger";
  }

  @Override
  public AuthorizationPlugin newPlugin(
      String metalake, String catalogProvider, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties.containsKey(RangerAuthorizationProperties.RANGER_SERVICE_TYPE),
        String.format(
            ErrorMessages.MISSING_REQUIRED_ARGUMENT,
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE));
    String serviceType =
        properties.get(RangerAuthorizationProperties.RANGER_SERVICE_TYPE).toUpperCase();
    switch (serviceType) {
      case "HADOOPSQL":
        return new RangerAuthorizationHadoopSQLPlugin(metalake, properties);
      case "HDFS":
        return new RangerAuthorizationHDFSPlugin(metalake, properties);
      default:
        throw new IllegalArgumentException("Unsupported service type: " + serviceType);
    }
  }
}
