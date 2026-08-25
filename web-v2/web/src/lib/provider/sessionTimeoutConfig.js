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

const SESSION_IDLE_TIMEOUT_KEY = 'gravitino.server.webserver.sessionIdleTimeoutMs'

const SESSION_MAX_DURATION_KEY = 'gravitino.server.webserver.sessionMaxDurationMs'

const SESSION_IDLE_WARNING_LEAD_KEY = 'gravitino.server.webserver.sessionIdleWarningLeadMs'

function resolveSessionDuration(serverValue, envValue, defaultValue) {
  const serverDuration = Number(serverValue)
  if (Number.isFinite(serverDuration) && serverDuration > 0) {
    return serverDuration
  }

  const envDuration = Number(envValue)

  return Number.isFinite(envDuration) && envDuration > 0 ? envDuration : defaultValue
}

/**
 * Resolves all UI session timeout settings.
 *
 * @param {Object} systemConfig values returned by the /configs endpoint
 * @param {Object} defaults built-in fallback values
 * @returns {{idleTimeoutMs: number, warningLeadMs: number, maxSessionDurationMs: number}}
 */
export function resolveSessionTimeouts(systemConfig, defaults) {
  return {
    idleTimeoutMs: resolveSessionDuration(
      systemConfig?.[SESSION_IDLE_TIMEOUT_KEY],
      process.env.NEXT_PUBLIC_IDLE_TIMEOUT_MS,
      defaults.idleTimeoutMs
    ),
    warningLeadMs: resolveSessionDuration(
      systemConfig?.[SESSION_IDLE_WARNING_LEAD_KEY],
      process.env.NEXT_PUBLIC_IDLE_WARNING_LEAD_MS,
      defaults.warningLeadMs
    ),
    maxSessionDurationMs: resolveSessionDuration(
      systemConfig?.[SESSION_MAX_DURATION_KEY],
      process.env.NEXT_PUBLIC_MAX_SESSION_DURATION_MS,
      defaults.maxSessionDurationMs
    )
  }
}
