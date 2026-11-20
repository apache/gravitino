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

import { describe, expect, it } from 'vitest'
import { gatewayDate, latestRequest, mcpAccessStatus } from './mcpAccess'

const gated = { id: 1, requires_approval: true }
const now = new Date('2026-10-12T12:00:00Z')

const request = (status, requestedAt, mcpId = 1) => ({ mcp_id: mcpId, status, requested_at: requestedAt })

describe('mcpAccessStatus', () => {
  it('is granted with a permission', () => {
    expect(mcpAccessStatus(gated, [{ mcp_id: 1, expires_at: null }], [], now)).toBe('granted')
  })

  it('is expired once the permission expires', () => {
    expect(mcpAccessStatus(gated, [{ mcp_id: 1, expires_at: '2026-10-12T11:59:00' }], [], now)).toBe('expired')
  })

  it('is open when the server needs no approval, whatever an old grant says', () => {
    const open = { id: 1, requires_approval: false }

    expect(mcpAccessStatus(open, [], [], now)).toBe('open')
    expect(mcpAccessStatus(open, [{ mcp_id: 1, expires_at: '2026-10-01T00:00:00' }], [], now)).toBe('open')
  })

  it('is pending, not expired, while a renewal waits for approval', () => {
    const expired = [{ mcp_id: 1, expires_at: '2026-10-01T00:00:00' }]

    expect(mcpAccessStatus(gated, expired, [request('pending', '2026-10-11T09:00:00')], now)).toBe('pending')
  })

  it('follows the latest request', () => {
    const requests = [request('denied', '2026-10-01T09:00:00'), request('pending', '2026-10-11T09:00:00')]

    expect(mcpAccessStatus(gated, [], requests, now)).toBe('pending')
  })

  it('is denied when the latest request was denied', () => {
    expect(mcpAccessStatus(gated, [], [request('denied', '2026-10-11T09:00:00')], now)).toBe('denied')
  })

  it('ignores requests for other servers and approvals that were revoked', () => {
    const requests = [request('pending', '2026-10-11T09:00:00', 2), request('approved', '2026-10-10T09:00:00')]

    expect(mcpAccessStatus(gated, [], requests, now)).toBe('none')
  })
})

describe('gatewayDate', () => {
  it('reads a timestamp without a zone as UTC', () => {
    expect(gatewayDate('2026-10-12T08:30:00').toISOString()).toBe('2026-10-12T08:30:00.000Z')
  })

  it('keeps an explicit zone', () => {
    expect(gatewayDate('2026-10-12T08:30:00+02:00').toISOString()).toBe('2026-10-12T06:30:00.000Z')
  })

  it('has no date for no value', () => {
    expect(gatewayDate(null)).toBeNull()
  })
})

describe('latestRequest', () => {
  it("is the user's latest request for the server, where the admin's note is", () => {
    const requests = [
      { mcp_id: 1, status: 'denied', requested_at: '2026-10-01T10:00:00', admin_notes: 'Old' },
      { mcp_id: 1, status: 'denied', requested_at: '2026-10-02T10:00:00', admin_notes: 'Ask your lead' },
      { mcp_id: 2, status: 'pending', requested_at: '2026-10-03T10:00:00' }
    ]

    expect(latestRequest({ id: 1 }, requests).admin_notes).toBe('Ask your lead')
    expect(latestRequest({ id: 3 }, requests)).toBeUndefined()
  })
})
