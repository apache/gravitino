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

// Periods the audit log shows, as the gateway's analytics summary takes them.
export const AUDIT_PERIODS = [
  { key: '1h', label: 'Last hour', params: { hours: 1 } },
  { key: '24h', label: 'Last 24 hours', params: { hours: 24 } },
  { key: '7d', label: 'Last 7 days', params: { days: 7 } },
  { key: '30d', label: 'Last 30 days', params: { days: 30 } }
]

export const DEFAULT_AUDIT_PERIOD = AUDIT_PERIODS.find(period => period.key === '24h')

// The most calls the gateway puts in one CSV export, the latest first.
export const EXPORT_ROW_LIMIT = 10000

const HOUR_MS = 60 * 60 * 1000

// `date` as the UTC timestamp without a time zone that the gateway stores.
const gatewayTime = date => date.toISOString().replace(/Z$/, '')

// The start of `period`, in the gateway's time.
export const periodStart = (period, now = new Date()) => {
  const { hours = 0, days = 0 } = period.params

  return gatewayTime(new Date(now.getTime() - (hours + days * 24) * HOUR_MS))
}

// The filters that narrow the summary as well as the list: every one but status.
const narrowingParams = ({ service, tool, userId }) => ({
  ...(service && { service }),
  ...(tool?.trim() && { tool_name: tool.trim() }),
  ...(userId && { user_id: userId })
})

/**
 * The gateway's audit log filters, leaving out the ones that aren't set; the CSV
 * export takes them too. The period ends at `now`, so that later calls don't
 * shift the pages.
 */
export const auditFilterParams = (filters, now = new Date()) => ({
  start_date: periodStart(filters.period, now),
  end_date: gatewayTime(now),
  ...narrowingParams(filters),
  ...(filters.status && { status: filters.status })
})

// One page of the gateway's audit log.
export const auditLogParams = (filters, page, pageSize, now = new Date()) => ({
  ...auditFilterParams(filters, now),
  limit: pageSize,
  offset: (page - 1) * pageSize
})

// The analytics summary of the calls the list shows, whatever their status, over the same window.
export const auditSummaryParams = (filters, now = new Date()) => {
  const { status, ...params } = auditFilterParams(filters, now)

  return params
}
