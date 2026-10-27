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
package org.apache.gravitino.maintenance.scheduler;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoAuthSettings;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Runs policy-expand / commit-chain resolve under a {@link UserPrincipal} built from {@code
 * gravitino.maintenance.gravitinoAuth.*}.
 *
 * <p>db-scheduler threads have no inbound HTTP {@code Authorization}. Lakehouse catalogs with
 * {@code catalog-backend=rest} forward the current principal's token to Iceberg REST via {@code
 * UserPrincipalForwardingAuthManager}; without a principal those calls fail with {@code
 * NotAuthorized}. When gravitinoAuth is unset or {@code none}, the action runs unchanged so jdbc /
 * hive backends and unauthenticated IT setups keep working.
 */
public final class MaintenanceAuthDoAs {

  private MaintenanceAuthDoAs() {}

  /**
   * Executes {@code action} under a gravitinoAuth principal when configured.
   *
   * @param config gravitino server config
   * @param action work that may call {@code internalTableDispatcher} / REST catalog backends
   * @param <T> result type
   * @return action result
   */
  public static <T> T call(@Nullable Config config, PrivilegedExceptionAction<T> action) {
    Optional<UserPrincipal> principal = resolvePrincipal(config);
    try {
      if (principal.isEmpty()) {
        return action.run();
      }
      return PrincipalUtils.doAs(principal.get(), action);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds a forwarding-capable principal from gravitinoAuth, or empty when auth is not configured.
   *
   * @param config gravitino server config
   * @return principal with Authorization header, or empty
   */
  static Optional<UserPrincipal> resolvePrincipal(@Nullable Config config) {
    if (config == null) {
      return Optional.empty();
    }
    Map<String, String> gravitinoAuth = MaintenanceAuthConfigs.loadGravitinoAuth(config);
    if (gravitinoAuth.isEmpty()) {
      return Optional.empty();
    }
    Map<String, String> properties = new HashMap<>(gravitinoAuth);
    GravitinoAuthSettings.copyAliases(properties);
    GravitinoAuthSettings settings = GravitinoAuthSettings.from(new OptimizerConfig(properties));
    if (!settings.hasAuth()) {
      return Optional.empty();
    }
    Optional<String> authorization = settings.authorizationHeader();
    if (authorization.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new UserPrincipal(settings.principalName(), authorization.get()));
  }
}
