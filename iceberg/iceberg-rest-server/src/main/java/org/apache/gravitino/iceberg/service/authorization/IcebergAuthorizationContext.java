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

package org.apache.gravitino.iceberg.service.authorization;

import com.google.common.base.Preconditions;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.service.provider.DynamicIcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;

public class IcebergAuthorizationContext {
  private boolean isAuthorizationEnabled;
  private String metalakeName;

  private IcebergAuthorizationContext(Boolean isAuthorizationEnabled, String metalakeName) {
    this.isAuthorizationEnabled = isAuthorizationEnabled;
    this.metalakeName = metalakeName;
  }

  private static class InstanceHolder {
    private static IcebergAuthorizationContext INSTANCE;
  }

  public static IcebergAuthorizationContext create(IcebergConfigProvider configProvider) {
    Boolean enableAuth = GravitinoEnv.getInstance().config().get(Configs.ENABLE_AUTHORIZATION);
    if (enableAuth) {
      Preconditions.checkArgument(
          configProvider instanceof DynamicIcebergConfigProvider,
          "Please enable dynamic config provider if using authorization.");
    }
    InstanceHolder.INSTANCE =
        new IcebergAuthorizationContext(enableAuth, configProvider.getMetalakeName());
    return InstanceHolder.INSTANCE;
  }

  public static IcebergAuthorizationContext getInstance() {
    Preconditions.checkState(InstanceHolder.INSTANCE != null, "Not initialized");
    return InstanceHolder.INSTANCE;
  }

  public boolean authorizationEnabled() {
    return isAuthorizationEnabled;
  }

  public String metalakeName() {
    return metalakeName;
  }
}
