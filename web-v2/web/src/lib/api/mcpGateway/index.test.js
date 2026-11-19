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
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createMcpGatewayApi, gatewayErrorMessage, isSignedOut } from './index'

vi.mock('@/lib/auth/providers/factory', () => ({
  oauthProviderFactory: { getAccessToken: vi.fn() }
}))

describe('createMcpGatewayApi', () => {
  const originalAdapter = axios.defaults.adapter
  const signedIn = async () => 'user-token'
  let requests

  beforeEach(() => {
    requests = []
    axios.defaults.adapter = async config => {
      requests.push(config)

      return { data: { ok: true }, status: 200, statusText: 'OK', headers: {}, config }
    }
  })

  afterEach(() => {
    axios.defaults.adapter = originalAdapter
  })

  it("sends the signed-in user's token to the gateway", async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await expect(api.getCatalog()).resolves.toEqual({ ok: true })

    const [request] = requests
    expect(request.baseURL).toBe('https://gateway.example.com')
    expect(request.url).toBe('/api/v1/catalog/mcps')
    expect(request.headers.Authorization).toBe('Bearer user-token')
  })

  it('does not call the gateway when no one is signed in', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', async () => null)

    const error = await api.getMe().catch(err => err)

    expect(isSignedOut(error)).toBe(true)
    expect(gatewayErrorMessage(error)).toBe('Sign in to Gravitino again')
    expect(requests).toEqual([])
    expect(isSignedOut({ response: { status: 401 } })).toBe(false)
  })

  it('builds MCP endpoint URLs from the configured URL', () => {
    const api = createMcpGatewayApi('https://gateway.example.com/', async () => null)

    expect(api.endpointUrl('gravitino')).toBe('https://gateway.example.com/mcp/gravitino')
  })

  it('escapes server ids in paths', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.updateServer('a/b', { is_active: false })

    expect(requests[0].url).toBe('/api/v1/admin/mcp/a%2Fb')
    expect(requests[0].method).toBe('patch')
  })

  it('asks for access with the server id and a justification', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.requestAccess(7, 'Investigating a data issue')

    expect(JSON.parse(requests[0].data)).toEqual({ mcp_id: 7, justification: 'Investigating a data issue' })
  })

  it('pages through pending access requests', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.getPendingRequests(2, 20)

    expect(requests[0].url).toBe('/api/v1/mcp-access/admin/pending')
    expect(requests[0].params).toEqual({ page: 2, page_size: 20 })
  })

  it('connects in the browser and comes back to the page that asked', () => {
    const api = createMcpGatewayApi('https://gateway.example.com/', async () => null)

    const url = new URL(api.connectUrl('a/b', 'http://ui:3000/mcp?tab=1'))

    expect(url.origin + url.pathname).toBe('https://gateway.example.com/portal/connect/a%2Fb')
    expect(url.searchParams.get('return_to')).toBe('http://ui:3000/mcp?tab=1')
  })

  it("disconnects with the signed-in user's token", async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.disconnect('docs')

    expect(requests[0].method).toBe('post')
    expect(requests[0].url).toBe('/portal/disconnect/docs')
    expect(requests[0].headers.Authorization).toBe('Bearer user-token')
  })

  it('pages through granted access and revokes it', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.getPermissions(3, 25)
    await api.revokePermission(12)

    expect(requests[0].url).toBe('/api/v1/mcp-access/admin/permissions')
    expect(requests[0].params).toEqual({ page: 3, page_size: 25 })
    expect(requests[1].method).toBe('delete')
    expect(requests[1].url).toBe('/api/v1/mcp-access/admin/permissions/12')
  })

  it('reads the audit log, one entry, and its summary', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.getAuditLogs({ service: 'gravitino', limit: 25, offset: 50 })
    await api.getAuditLog(9)
    await api.getAuditSummary({ days: 7 })

    expect(requests[0].url).toBe('/api/v1/audit/logs')
    expect(requests[0].params).toEqual({ service: 'gravitino', limit: 25, offset: 50 })
    expect(requests[1].url).toBe('/api/v1/audit/logs/9')
    expect(requests[2].url).toBe('/api/v1/audit/analytics/summary')
    expect(requests[2].params).toEqual({ days: 7 })
  })

  it('downloads the audit log as a file', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    await api.exportAuditLogs({ days: 30 })

    expect(requests[0].url).toBe('/api/v1/audit/logs/export/csv')
    expect(requests[0].params).toEqual({ days: 30 })
    expect(requests[0].responseType).toBe('blob')
    expect(requests[0].headers.Authorization).toBe('Bearer user-token')
  })

  it("reports the gateway's message when a download fails", async () => {
    axios.defaults.adapter = async () => {
      // jsdom's Blob lacks text(), which browsers have.
      const json = JSON.stringify({ detail: 'Access denied' })
      const body = Object.assign(new Blob([json], { type: 'application/json' }), { text: async () => json })
      throw Object.assign(new Error('Request failed with status code 403'), { response: { status: 403, data: body } })
    }
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)

    const error = await api.exportAuditLogs({ days: 30 }).catch(failure => failure)

    expect(gatewayErrorMessage(error)).toBe('Access denied')
    expect(error.response.status).toBe(403)
  })

  it('manages service accounts', async () => {
    const api = createMcpGatewayApi('https://gateway.example.com', signedIn)
    const account = { name: 'svc-etl', oauth_credentials: { service_account_name: 'etl@proj.iam' } }

    await api.getServiceAccounts()
    await api.createServiceAccount(account)
    await api.updateServiceAccount(4, { allowed_services: ['gravitino'] })
    await api.deleteServiceAccount(4)

    expect(requests.map(request => [request.method, request.url])).toEqual([
      ['get', '/api/v1/admin/service-accounts'],
      ['post', '/api/v1/admin/service-accounts'],
      ['patch', '/api/v1/admin/service-accounts/4'],
      ['delete', '/api/v1/admin/service-accounts/4']
    ])
    expect(JSON.parse(requests[1].data)).toEqual(account)
    expect(JSON.parse(requests[2].data)).toEqual({ allowed_services: ['gravitino'] })
  })
})

describe('gatewayErrorMessage', () => {
  it.each([
    [
      'a message from the gateway',
      { response: { data: { detail: 'Admin privileges required' } } },
      'Admin privileges required'
    ],
    [
      'validation errors',
      { response: { data: { detail: [{ msg: 'name is too short' }, { msg: 'mcp_url is required' }] } } },
      'name is too short; mcp_url is required'
    ],
    [
      'a validator error, without its prefix',
      { response: { data: { detail: [{ msg: 'Value error, Secrets must be named with a prefix' }] } } },
      'Secrets must be named with a prefix'
    ],
    [
      'a field error, under the field it is about',
      {
        response: {
          data: { detail: [{ loc: ['body', 'oauth_metadata_url'], msg: 'Value error, must be an http or https URL' }] }
        }
      },
      'oauth_metadata_url: must be an http or https URL'
    ],
    [
      'an error about the whole request, without a field',
      {
        response: {
          data: { detail: [{ loc: ['body'], msg: 'Value error, A client secret needs oauth_metadata_url' }] }
        }
      },
      'A client secret needs oauth_metadata_url'
    ],
    ['a network failure', { message: 'Network Error' }, 'Network Error'],
    ['nothing at all', undefined, 'The MCP gateway did not respond']
  ])('describes %s', (_, error, expected) => {
    expect(gatewayErrorMessage(error)).toBe(expected)
  })
})

describe('createMcpGatewayApi', () => {
  it.each([
    'javascript:alert(1)',
    'gateway.example.com',
    'ftp://gateway.example.com',

    // Over plain http elsewhere, the user's token would cross the network in clear.
    'http://gateway.example.com',
    'http://localhost.example.com'
  ])('refuses %s', uri => {
    expect(() => createMcpGatewayApi(uri, async () => 'token')).toThrow('https URL, or http on this machine')
  })

  it.each(['https://gateway.example.com', 'http://localhost:8000', 'http://127.0.0.1:8000/', 'http://[::1]'])(
    'accepts %s',
    uri => {
      expect(() => createMcpGatewayApi(uri, async () => 'token')).not.toThrow()
    }
  )
})
