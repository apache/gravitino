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

import { useCallback, useState } from 'react'
import { Button, Flex, Popconfirm, Spin, Table, Tabs, Tag, Typography } from 'antd'
import toast from 'react-hot-toast'
import SectionContainer from '@/components/SectionContainer'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { formatToDateTime } from '@/lib/utils/date'
import { gatewayDate } from '@/lib/utils/mcpAccess'
import { useMcpGateway, AdminOnly } from '../McpGatewayProvider'
import useLastPageWhenEmpty from '../useLastPageWhenEmpty'
import useLatestResult from '../useLatestResult'
import ReviewDialog from './ReviewDialog'

const { Title, Paragraph, Text } = Typography

const STATUS_COLORS = { pending: 'gold', approved: 'green', denied: 'red' }

const NO_ROWS = { items: [], total: 0 }

const when = value => (value ? formatToDateTime(gatewayDate(value)) : '-')

const hasExpired = permission => Boolean(permission.expires_at) && gatewayDate(permission.expires_at) < new Date()

export default function AccessRequestsPage() {
  return (
    <AdminOnly>
      <AccessRequests />
    </AdminOnly>
  )
}

function AccessRequests() {
  const { api } = useMcpGateway()
  const [tab, setTab] = useState('pending')
  const [page, setPage] = useState({ current: 1, pageSize: 25 })
  const [review, setReview] = useState()
  const [reloads, setReloads] = useState(0)
  const load = () => setReloads(count => count + 1)

  // Only the answer for this tab and page is shown, however the answers arrive.
  const { loading, result: data = NO_ROWS } = useLatestResult(
    useCallback(() => {
      const fetchPage = { pending: api.getPendingRequests, all: api.getAllRequests, granted: api.getPermissions }[tab]

      return fetchPage(page.current, page.pageSize)
    }, [api, tab, page, reloads]),
    `${tab}:${page.current}:${page.pageSize}`
  )
  useLastPageWhenEmpty(loading, data.items.length, data.total, page, setPage)

  const revoke = async permission => {
    try {
      await api.revokePermission(permission.id)
      toast.success(`Revoked ${permission.username}'s access to ${permission.mcp_name}`)
      load()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    }
  }

  const grantedColumns = [
    {
      title: 'User',
      key: 'user',
      width: 220,
      render: (_, permission) => (
        <>
          <div>{permission.username}</div>
          {permission.email && permission.email !== permission.username && (
            <Text type='secondary'>{permission.email}</Text>
          )}
        </>
      )
    },
    {
      title: 'Server',
      key: 'server',
      width: 200,
      render: (_, permission) => `${permission.mcp_icon || ''} ${permission.mcp_name}`
    },
    {
      title: 'Granted',
      key: 'granted',
      render: (_, permission) =>
        permission.granted_by
          ? `${when(permission.granted_at)} by ${permission.granted_by}`
          : when(permission.granted_at)
    },
    {
      title: 'Expires',
      key: 'expires',
      width: 220,
      render: (_, permission) => (
        <Flex gap='small' align='center'>
          {permission.expires_at ? when(permission.expires_at) : 'Never'}
          {hasExpired(permission) && <Tag>Expired</Tag>}
        </Flex>
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 110,
      fixed: 'right',
      render: (_, permission) => (
        <Popconfirm
          title={`Revoke ${permission.username}'s access to ${permission.mcp_name}?`}
          description='They can no longer use the server unless it is open to everyone.'
          okText='Revoke'
          okButtonProps={{ danger: true }}
          onConfirm={() => revoke(permission)}
        >
          <Button size='small' danger>
            Revoke
          </Button>
        </Popconfirm>
      )
    }
  ]

  const requestColumns = [
    { title: 'User', dataIndex: 'username', key: 'user', width: 160 },
    {
      title: 'Server',
      key: 'server',
      width: 200,
      render: (_, request) => `${request.mcp_icon || ''} ${request.mcp_name}`
    },
    {
      title: 'Justification',
      dataIndex: 'justification',
      key: 'justification',
      render: value => <Text ellipsis={{ tooltip: value }}>{value || '-'}</Text>
    },
    { title: 'Requested', key: 'requested', width: 180, render: (_, request) => when(request.requested_at) }
  ]

  const actionsColumn = {
    title: 'Actions',
    key: 'actions',
    width: 180,
    fixed: 'right',
    render: (_, request) => (
      <Flex gap='small'>
        <Button size='small' type='primary' onClick={() => setReview({ request, status: 'approved' })}>
          Approve
        </Button>
        <Button size='small' danger onClick={() => setReview({ request, status: 'denied' })}>
          Deny
        </Button>
      </Flex>
    )
  }

  const reviewColumns = [
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 110,
      render: value => <Tag color={STATUS_COLORS[value]}>{value}</Tag>
    },
    {
      title: 'Reviewed',
      key: 'reviewed',
      width: 220,
      render: (_, request) =>
        request.reviewed_at ? `${when(request.reviewed_at)} by ${request.reviewer_username || '-'}` : '-'
    },
    {
      title: 'Note',
      dataIndex: 'admin_notes',
      key: 'note',
      render: value => <Text ellipsis={{ tooltip: value }}>{value || '-'}</Text>
    }
  ]
  const columns = tab === 'pending' ? [...requestColumns, actionsColumn] : [...requestColumns, ...reviewColumns]

  return (
    <SectionContainer classProps='block overflow-y-auto'>
      <Title level={2}>Access Requests</Title>
      <Paragraph type='secondary'>
        Requests from users for access to MCP servers that require approval, and the access they have been granted.
      </Paragraph>
      <Tabs
        activeKey={tab}
        onChange={key => {
          setTab(key)
          setPage(current => ({ ...current, current: 1 }))
        }}
        items={[
          { key: 'pending', label: 'Pending' },
          { key: 'all', label: 'All' },
          { key: 'granted', label: 'Granted' }
        ]}
      />
      <Spin spinning={loading}>
        <Table
          dataSource={data.items.map(item => ({ ...item, key: item.id }))}
          columns={tab === 'granted' ? grantedColumns : columns}
          tableLayout='fixed'
          scroll={{ x: 960 }}
          pagination={{
            ...page,
            total: data.total,
            position: ['bottomCenter'],
            showSizeChanger: true,
            onChange: (current, pageSize) => setPage({ current, pageSize })
          }}
        />
      </Spin>
      {review && (
        <ReviewDialog
          request={review.request}
          status={review.status}
          onClose={() => setReview(undefined)}
          onReviewed={() => {
            setReview(undefined)
            load()
          }}
        />
      )}
    </SectionContainer>
  )
}
