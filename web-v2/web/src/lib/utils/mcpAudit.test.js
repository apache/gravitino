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
import {
  AUDIT_PERIODS,
  auditFilterParams,
  auditLogParams,
  auditSummaryParams,
  DEFAULT_AUDIT_PERIOD,
  periodStart
} from './mcpAudit'

const NOW = new Date('2026-10-22T12:00:00Z')
const period = key => AUDIT_PERIODS.find(item => item.key === key)

describe('periodStart', () => {
  it("is the gateway's UTC time, without a time zone, a period ago", () => {
    expect(periodStart(period('1h'), NOW)).toBe('2026-10-22T11:00:00.000')
    expect(periodStart(period('7d'), NOW)).toBe('2026-10-15T12:00:00.000')
  })
})

describe('auditLogParams', () => {
  it('leaves out filters that are not set', () => {
    expect(auditLogParams({ period: period('24h') }, 1, 25, NOW)).toEqual({
      start_date: '2026-10-21T12:00:00.000',
      end_date: '2026-10-22T12:00:00.000',
      limit: 25,
      offset: 0
    })
  })

  it('passes every filter that is set, and pages by offset', () => {
    const filters = { period: period('30d'), service: 'gravitino', status: 'error', tool: ' get_table ', userId: 4 }

    expect(auditLogParams(filters, 3, 50, NOW)).toEqual({
      start_date: '2026-09-22T12:00:00.000',
      end_date: '2026-10-22T12:00:00.000',
      service: 'gravitino',
      status: 'error',
      tool_name: 'get_table',
      user_id: 4,
      limit: 50,
      offset: 100
    })
  })
})

describe('auditFilterParams', () => {
  it('is the log query without paging, for the CSV export', () => {
    const filters = { period: period('1h'), status: 'success', userId: 7 }

    expect(auditFilterParams(filters, NOW)).toEqual({
      start_date: '2026-10-22T11:00:00.000',
      end_date: '2026-10-22T12:00:00.000',
      status: 'success',
      user_id: 7
    })
  })
})

describe('auditSummaryParams', () => {
  it("narrows the summary like the list, except by status, so its figures count the list's calls", () => {
    const filters = { period: period('7d'), service: 'gravitino', status: 'error', tool: ' get_table ', userId: 4 }

    const now = new Date('2026-10-22T12:00:00Z')

    expect(auditSummaryParams(filters, now)).toEqual({
      start_date: '2026-10-15T12:00:00.000',
      end_date: '2026-10-22T12:00:00.000',
      service: 'gravitino',
      tool_name: 'get_table',
      user_id: 4
    })
  })

  it('takes the window of the list', () => {
    const filters = { period: DEFAULT_AUDIT_PERIOD }
    const now = new Date('2026-10-22T12:00:00Z')

    expect(auditSummaryParams(filters, now)).toEqual({
      start_date: auditFilterParams(filters, now).start_date,
      end_date: auditFilterParams(filters, now).end_date
    })
  })
})
