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
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Modal, Spin, Table, Tag, Tooltip, Typography } from 'antd'
import toast from 'react-hot-toast'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import { ROUTES } from '@/config/routes'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { formatToDateTime } from '@/lib/utils/date'
import { gatewayDate } from '@/lib/utils/mcpAccess'
import { useMcpGateway, AdminOnly } from '../McpGatewayProvider'
import useLatestResult from '../useLatestResult'
import ServiceAccountDialog, { PROVIDERS } from './ServiceAccountDialog'

const { Title, Paragraph, Text } = Typography

const providerLabel = provider => PROVIDERS.find(item => item.value === provider)?.label || provider

export default function ServiceAccountsPage() {
  return (
    <AdminOnly>
      <ServiceAccounts />
    </AdminOnly>
  )
}

function ServiceAccounts() {
  const { api } = useMcpGateway()
  const metalake = useSearchParams().get('metalake')
  const [modal, contextHolder] = Modal.useModal()

  // The account being edited, or null while adding one.
  const [editing, setEditing] = useState()

  const [reloads, setReloads] = useState(0)
  const load = () => setReloads(count => count + 1)

  const { loading, result: accounts = [] } = useLatestResult(
    useCallback(() => api.getServiceAccounts(), [api, reloads]),
    'accounts'
  )

  const remove = async account => {
    try {
      await api.deleteServiceAccount(account.id)
      toast.success(`Deleted ${account.username}`)
      load()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    }
  }

  // The audit log of one account, keeping the metalake the rest of the UI is on.
  const auditLogHref = account =>
    `${ROUTES.McpAudit}?${new URLSearchParams({ user: account.id, name: account.username, ...(metalake && { metalake }) })}`

  const confirmDelete = account => {
    let validate = () => false
    modal.confirm({
      title: `Delete ${account.username}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <>
          <Paragraph>Its workload can no longer call MCP servers. Its audit history is kept.</Paragraph>
          <ConfirmInput
            name={account.username}
            setConfirmInput={() => {}}
            registerValidate={fn => {
              validate = fn
            }}
          />
        </>
      ),
      okText: 'Delete',
      okType: 'danger',
      onOk(close) {
        if (!validate()) return
        close()
        remove(account)
      }
    })
  }

  const columns = [
    {
      title: 'Service account',
      key: 'name',
      sorter: (a, b) => a.username.localeCompare(b.username),
      render: (_, account) => (
        <>
          <div className='font-medium'>{account.username}</div>
          <Text type='secondary' ellipsis={{ tooltip: account.service_account_name }}>
            {account.service_account_name}
          </Text>
        </>
      )
    },
    { title: 'Team', dataIndex: 'team', key: 'team', width: 140, render: value => value || '-' },
    { title: 'Runs on', key: 'provider', width: 180, render: (_, account) => providerLabel(account.provider) },
    {
      title: 'Servers',
      key: 'servers',
      width: 220,
      render: (_, account) =>
        account.allowed_services.length ? (
          account.allowed_services.map(service => <Tag key={service}>{service}</Tag>)
        ) : (
          <Text type='secondary'>None</Text>
        )
    },
    {
      title: 'Added',
      key: 'created',
      width: 180,
      render: (_, account) => formatToDateTime(gatewayDate(account.created_at))
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      fixed: 'right',
      render: (_, account) => (
        <Flex gap={4} align='center'>
          <Tooltip title='Edit'>
            <Button
              type='text'
              size='small'
              aria-label={`Edit ${account.username}`}
              icon={<Icons.Pencil className='size-4' />}
              onClick={() => setEditing(account)}
            />
          </Tooltip>
          <Tooltip title='Audit log'>
            <Link
              href={auditLogHref(account)}
              aria-label={`Audit log of ${account.username}`}
              className='inline-flex p-1'
            >
              <Icons.ScrollText className='size-4' />
            </Link>
          </Tooltip>
          <Tooltip title='Delete'>
            <Button
              type='text'
              size='small'
              aria-label={`Delete ${account.username}`}
              icon={<Icons.Trash2Icon className='size-4' />}
              onClick={() => confirmDelete(account)}
            />
          </Tooltip>
        </Flex>
      )
    }
  ]

  return (
    <SectionContainer classProps='block overflow-y-auto'>
      {contextHolder}
      <Title level={2}>Service Accounts</Title>
      <Paragraph type='secondary'>
        Workloads that call MCP servers through the gateway with their platform identity token, such as a GKE or EKS
        workload.
      </Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <Button type='primary' icon={<PlusOutlined />} onClick={() => setEditing(null)}>
          Add Service Account
        </Button>
      </Flex>
      <Spin spinning={loading}>
        <Table
          dataSource={accounts.map(account => ({ ...account, key: account.id }))}
          columns={columns}
          tableLayout='fixed'
          scroll={{ x: 960 }}
          pagination={{ position: ['bottomCenter'] }}
        />
      </Spin>
      {editing !== undefined && (
        <ServiceAccountDialog
          account={editing}
          onClose={() => setEditing(undefined)}
          onSaved={() => {
            setEditing(undefined)
            load()
          }}
        />
      )}
    </SectionContainer>
  )
}
