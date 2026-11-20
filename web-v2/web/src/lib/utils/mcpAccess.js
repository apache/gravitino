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

// The MCP gateway sends UTC timestamps without a time zone.
export const gatewayDate = value => {
  if (!value) {
    return null
  }

  return new Date(/(?:[zZ]|[+-]\d{2}:?\d{2})$/.test(value) ? value : `${value}Z`)
}

// The user's latest request for access to an MCP server, or undefined.
export const latestRequest = (server, requests = []) =>
  requests
    .filter(item => item.mcp_id === server.id)
    .sort((a, b) => gatewayDate(b.requested_at) - gatewayDate(a.requested_at))[0]

/**
 * How the signed-in user stands with an MCP server: 'open' (anyone may use it),
 * 'granted', 'pending', 'expired', 'denied' or 'none', as the gateway decides it.
 */
export const mcpAccessStatus = (server, permissions = [], requests = [], now = new Date()) => {
  // An open server takes no requests, and the gateway ignores old grants' expiry.
  if (!server.requires_approval) {
    return 'open'
  }

  const permission = permissions.find(item => item.mcp_id === server.id)
  const expiresAt = gatewayDate(permission?.expires_at)
  if (permission && !(expiresAt && expiresAt < now)) {
    return 'granted'
  }

  const latest = latestRequest(server, requests)

  // A renewal already asked for is pending, and can't be asked for again.
  if (latest?.status === 'pending') {
    return 'pending'
  }
  if (permission) {
    return 'expired'
  }

  return latest?.status === 'denied' ? 'denied' : 'none'
}
