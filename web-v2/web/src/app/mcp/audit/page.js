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

'use client'

import { useCallback, useEffect, useState } from 'react'
import { useSearchParams } from 'next/navigation'
import { DownloadOutlined, ReloadOutlined } from '@ant-design/icons'
import { Button, Card, Flex, Input, Select, Spin, Statistic, Table, Tag, Tooltip, Typography } from 'antd'
import toast from 'react-hot-toast'
import SectionContainer from '@/components/SectionContainer'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { formatToDateTime } from '@/lib/utils/date'
import { gatewayDate } from '@/lib/utils/mcpAccess'
import {
  AUDIT_PERIODS,
  DEFAULT_AUDIT_PERIOD,
  EXPORT_ROW_LIMIT,
  auditFilterParams,
  auditLogParams,
  auditSummaryParams
} from '@/lib/utils/mcpAudit'
import { useMcpGateway } from '../McpGatewayProvider'
import useLastPageWhenEmpty from '../useLastPageWhenEmpty'
import useLatestResult from '../useLatestResult'
import AuditDetail, { STATUS_COLORS } from './AuditDetail'

const { Title, Paragraph } = Typography

const STATUSES = ['success', 'error', 'unauthorized', 'cancelled']

// How long the browser has to start a download before its object URL is revoked.
const DOWNLOAD_URL_LIFETIME_MS = 60 * 1000

// Starts a download of `blob` as `name`; the gateway's file needs the user's token, so a plain link won't do.
const download = (blob, name) => {
  const url = URL.createObjectURL(blob)
  const link = Object.assign(document.createElement('a'), { href: url, download: name })
  document.body.appendChild(link)
  link.click()
  link.remove()
  setTimeout(() => URL.revokeObjectURL(url), DOWNLOAD_URL_LIFETIME_MS)
}

export default function AuditLogPage() {
  // A link to one account's audit log starts the page afresh.
  return <AuditLog key={useSearchParams().get('user') || ''} />
}

function AuditLog() {
  const { api, me } = useMcpGateway()
  const searchParams = useSearchParams()

  const [filters, setFilters] = useState(() => ({
    period: DEFAULT_AUDIT_PERIOD,
    userId: me.is_admin ? Number(searchParams.get('user')) || undefined : undefined,
    userName: searchParams.get('name') || undefined,
    asOf: new Date()
  }))
  const [page, setPage] = useState({ current: 1, pageSize: 25 })
  const [services, setServices] = useState([])
  const [detail, setDetail] = useState()
  const [isExporting, setIsExporting] = useState(false)

  // A change of filters, or a refresh, goes back to the first page of the calls up to now.
  const filter = changes => {
    setFilters(current => ({ ...current, ...changes, asOf: new Date() }))
    setPage(current => ({ ...current, current: 1 }))
  }

  // What the list shows; a refresh reloads the same view.
  const view = JSON.stringify({ ...filters, period: filters.period.key, asOf: undefined })

  const logs = useLatestResult(
    useCallback(
      () => api.getAuditLogs(auditLogParams(filters, page.current, page.pageSize, filters.asOf)),
      [api, filters, page]
    ),
    `${view}:${page.current}:${page.pageSize}`
  )
  useLastPageWhenEmpty(logs.loading, logs.result?.logs?.length ?? 0, logs.result?.total ?? 0, page, setPage)

  const summary = useLatestResult(
    useCallback(() => api.getAuditSummary(auditSummaryParams(filters, filters.asOf)), [api, filters]),
    view
  )
  const closeDetail = useCallback(() => setDetail(undefined), [])

  useEffect(() => {
    const listServers = me.is_admin ? api.getServers() : api.getCatalog()
    listServers
      .then(list => setServices(list.map(server => server.service_id)))
      .catch(error => toast.error(`Could not list the servers: ${gatewayErrorMessage(error)}`))
  }, [api, me.is_admin])

  const exportCsv = async () => {
    setIsExporting(true)
    try {
      download(await api.exportAuditLogs(auditFilterParams(filters, filters.asOf)), 'mcp-gateway-audit.csv')
      const total = logs.result?.total ?? 0
      if (total > EXPORT_ROW_LIMIT) {
        toast(`The file holds the latest ${EXPORT_ROW_LIMIT.toLocaleString()} of ${total.toLocaleString()} calls.`)
      }
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    } finally {
      setIsExporting(false)
    }
  }

  const figures = summary.result
  const hasCalls = Boolean(figures?.total_calls)

  // Failed calls, refused ones and ones whose client left.
  const unsuccessful = figures ? figures.total_calls - figures.successful_calls : '-'

  const columns = [
    {
      title: 'Time',
      key: 'time',
      width: 180,
      render: (_, log) => formatToDateTime(gatewayDate(log.created_at))
    },
    {
      title: 'Account',
      key: 'account',
      width: 200,
      ellipsis: true,
      render: (_, log) =>
        me.is_admin ? (
          <Button
            type='link'
            size='small'
            className='h-auto p-0'
            title='Show only this account'
            onClick={() => filter({ userId: log.user_id, userName: log.username })}
          >
            {log.username}
          </Button>
        ) : (
          log.username
        )
    },
    { title: 'Server', dataIndex: 'service', key: 'service', width: 160, ellipsis: true },
    { title: 'Tool', dataIndex: 'tool_name', key: 'tool', ellipsis: { showTitle: true } },
    {
      title: 'Status',
      key: 'status',
      width: 120,
      render: (_, log) => (
        <Tooltip title={log.error_message}>
          <Tag color={STATUS_COLORS[log.status]}>{log.status}</Tag>
        </Tooltip>
      )
    },
    {
      title: 'Duration',
      key: 'duration',
      width: 110,
      render: (_, log) => (log.duration_ms === null ? '-' : `${log.duration_ms} ms`)
    },
    {
      title: <span className='sr-only'>Details</span>,
      key: 'details',
      width: 90,
      fixed: 'right',
      render: (_, log) => (
        <Button type='link' size='small' className='h-auto p-0' onClick={() => setDetail(log.id)}>
          Details
        </Button>
      )
    }
  ]

  return (
    <SectionContainer classProps='block overflow-y-auto'>
      <Title level={2}>Audit Log</Title>
      <Paragraph type='secondary'>
        {me.is_admin
          ? 'Every tool call made through the MCP gateway, by people and service accounts.'
          : 'The tool calls you made through the MCP gateway.'}
      </Paragraph>
      <Flex gap='middle' wrap className='mb-4'>
        <Card size='small'>
          <Statistic
            title={`Calls, ${filters.period.label.toLowerCase()}`}
            value={figures?.total_calls ?? '-'}
            loading={summary.loading}
          />
        </Card>
        <Card size='small'>
          <Statistic
            title='Succeeded'
            value={hasCalls ? figures.success_rate : '-'}
            suffix={hasCalls && '%'}
            loading={summary.loading}
          />
        </Card>
        <Card size='small'>
          <Statistic
            title='Average time'
            value={hasCalls ? Math.round(figures.average_response_time_ms) : '-'}
            suffix={hasCalls && 'ms'}
            loading={summary.loading}
          />
        </Card>
        <Card size='small'>
          <Statistic title='Not succeeded' value={unsuccessful} loading={summary.loading} />
        </Card>
      </Flex>
      <Flex gap='small' wrap align='center' className='mb-4'>
        <Select
          aria-label='Period'
          value={filters.period.key}
          style={{ width: 160 }}
          options={AUDIT_PERIODS.map(period => ({ value: period.key, label: period.label }))}
          onChange={key => filter({ period: AUDIT_PERIODS.find(period => period.key === key) })}
        />
        <Select
          allowClear
          showSearch
          aria-label='Server'
          placeholder='All servers'
          value={filters.service}
          style={{ width: 180 }}
          options={services.map(service => ({ value: service, label: service }))}
          onChange={service => filter({ service })}
        />
        <Select
          allowClear
          aria-label='Status'
          placeholder='Any status'
          value={filters.status}
          style={{ width: 140 }}
          options={STATUSES.map(status => ({ value: status, label: status }))}
          onChange={status => filter({ status })}
        />
        <Input.Search
          allowClear
          aria-label='Tool'
          placeholder='Exact tool name'
          style={{ width: 200 }}
          onSearch={tool => filter({ tool })}
        />
        {filters.userId && (
          <Tag closable onClose={() => filter({ userId: undefined, userName: undefined })}>
            Account: {filters.userName || filters.userId}
          </Tag>
        )}
        <Flex gap='small' className='ml-auto'>
          <Button icon={<ReloadOutlined />} onClick={() => filter({})}>
            Refresh
          </Button>
          <Button icon={<DownloadOutlined />} loading={isExporting} onClick={exportCsv}>
            Export CSV
          </Button>
        </Flex>
      </Flex>
      <Spin spinning={logs.loading}>
        <Table
          dataSource={(logs.result?.logs ?? []).map(log => ({ ...log, key: log.id }))}
          columns={columns}
          tableLayout='fixed'
          scroll={{ x: 960 }}
          pagination={{
            ...page,
            total: logs.result?.total ?? 0,
            position: ['bottomCenter'],
            showSizeChanger: true,
            onChange: (current, pageSize) => setPage({ current, pageSize })
          }}
        />
      </Spin>
      {detail && <AuditDetail logId={detail} onClose={closeDetail} />}
    </SectionContainer>
  )
}
