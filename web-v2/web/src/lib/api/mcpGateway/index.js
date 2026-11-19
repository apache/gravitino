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

import axios from 'axios'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

// The server configuration key holding the MCP gateway's URL.
export const MCP_GATEWAY_URI_KEY = 'gravitino.mcpGateway.uri'

const SIGNED_OUT = 'MCP_GATEWAY_SIGNED_OUT'

// A failed download's body arrives as a Blob; this reads it, so the gateway's own message shows.
const readBlobError = async error => {
  const body = error?.response?.data
  if (!(body instanceof Blob)) {
    return error
  }
  try {
    const data = JSON.parse(await body.text())

    return Object.assign(new Error(error.message), { response: { ...error.response, data } })
  } catch {
    return error
  }
}

/**
 * A client for the MCP gateway's API, which runs on its own origin and accepts
 * the access token of the user signed in to this UI.
 */
export const createMcpGatewayApi = (gatewayUri, getAccessToken = () => oauthProviderFactory.getAccessToken()) => {
  // The browser is sent to the gateway to connect servers, so it must be a web address,
  // and the user's token goes there: over https, or plain http only on this machine.
  if (
    !/^https:\/\//i.test(gatewayUri) &&
    !/^http:\/\/(localhost|127\.0\.0\.1|\[::1\])(:\d+)?(\/|$)/i.test(gatewayUri)
  ) {
    throw new Error(`gravitino.mcpGateway.uri must be an https URL, or http on this machine: ${gatewayUri}`)
  }
  const baseUrl = gatewayUri.replace(/\/+$/, '')
  const http = axios.create({ baseURL: baseUrl, headers: { Accept: 'application/json' } })

  http.interceptors.request.use(async config => {
    const token = await getAccessToken()
    if (!token) {
      throw Object.assign(new Error('Sign in to Gravitino again'), { code: SIGNED_OUT })
    }
    config.headers.Authorization = `Bearer ${token}`

    return config
  })

  const data = request => request.then(res => res.data)
  const server = serviceId => `/api/admin/mcp/${encodeURIComponent(serviceId)}`

  return {
    // Where MCP clients connect to a server.
    endpointUrl: serviceId => `${baseUrl}/mcp/${encodeURIComponent(serviceId)}`,

    // Where the browser connects to a server that has its own sign-in. The gateway
    // signs the user in if needed and sends the browser back to `returnTo`.
    connectUrl: (serviceId, returnTo) =>
      `${baseUrl}/portal/connect/${encodeURIComponent(serviceId)}?${new URLSearchParams({ return_to: returnTo })}`,
    disconnect: serviceId => data(http.post(`/portal/disconnect/${encodeURIComponent(serviceId)}`)),
    getMe: () => data(http.get('/api/users/me')),
    getCatalog: () => data(http.get('/api/catalog/mcps')),
    getMyPermissions: () => data(http.get('/api/mcp-access/my-permissions')),
    getMyRequests: () => data(http.get('/api/mcp-access/my-requests')),
    requestAccess: (mcpId, justification) =>
      data(http.post('/api/mcp-access/request', { mcp_id: mcpId, justification })),
    getServers: () => data(http.get('/api/admin/mcp/', { params: { include_inactive: true } })),
    createServer: fields => data(http.post('/api/admin/mcp/', fields)),
    updateServer: (serviceId, changes) => data(http.patch(server(serviceId), changes)),
    deleteServer: serviceId => data(http.delete(server(serviceId))),
    getPendingRequests: (page, pageSize) =>
      data(http.get('/api/mcp-access/admin/pending', { params: { page, page_size: pageSize } })),
    getAllRequests: (page, pageSize) =>
      data(http.get('/api/mcp-access/admin/all', { params: { page, page_size: pageSize } })),
    reviewRequest: (requestId, review) =>
      data(http.post(`/api/mcp-access/admin/review/${encodeURIComponent(requestId)}`, review)),
    getPermissions: (page, pageSize) =>
      data(http.get('/api/mcp-access/admin/permissions', { params: { page, page_size: pageSize } })),
    revokePermission: permissionId =>
      data(http.delete(`/api/mcp-access/admin/permissions/${encodeURIComponent(permissionId)}`)),

    // The audit log: everyone's calls for admins, and their own for everyone else.
    getAuditLogs: params => data(http.get('/api/audit/logs', { params })),
    getAuditLog: logId => data(http.get(`/api/audit/logs/${encodeURIComponent(logId)}`)),
    getAuditSummary: params => data(http.get('/api/audit/analytics/summary', { params })),
    exportAuditLogs: params =>
      data(http.get('/api/audit/logs/export/csv', { params, responseType: 'blob' })).catch(async error => {
        throw await readBlobError(error)
      }),

    getServiceAccounts: () => data(http.get('/api/admin/service-accounts')),
    createServiceAccount: fields => data(http.post('/api/admin/service-accounts', fields)),
    updateServiceAccount: (accountId, changes) =>
      data(http.patch(`/api/admin/service-accounts/${encodeURIComponent(accountId)}`, changes)),
    deleteServiceAccount: accountId => data(http.delete(`/api/admin/service-accounts/${encodeURIComponent(accountId)}`))
  }
}

// Whether a gateway call was not sent because no one is signed in to this UI.
export const isSignedOut = error => error?.code === SIGNED_OUT

// A readable message from a failed gateway call.
export const gatewayErrorMessage = error => {
  const detail = error?.response?.data?.detail
  if (typeof detail === 'string') {
    return detail
  }
  if (Array.isArray(detail)) {
    // The gateway's own checks come back as pydantic's "Value error, <message>",
    // under the name of the field they are about.
    return detail
      .map(item => {
        const message = String(item.msg ?? '').replace(/^Value error, /, '')
        const field = Array.isArray(item.loc) ? item.loc.at(-1) : undefined

        return field && field !== 'body' ? `${field}: ${message}` : message
      })
      .join('; ')
  }

  return error?.message || 'The MCP gateway did not respond'
}
