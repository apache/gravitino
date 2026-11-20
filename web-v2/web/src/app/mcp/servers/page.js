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
import { ExclamationCircleFilled, PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Modal, Spin, Table, Tag, Tooltip, Typography } from 'antd'
import toast from 'react-hot-toast'
import ConfirmInput from '@/components/ConfirmInput'
import Icons from '@/components/Icons'
import SectionContainer from '@/components/SectionContainer'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { useMcpGateway, AdminOnly } from '../McpGatewayProvider'
import useLatestResult from '../useLatestResult'
import ServerDialog from './ServerDialog'

const { Title, Paragraph, Text } = Typography

const signIn = server => {
  if (server.auth_type === 'oauth') {
    return server.auto_sso_enabled ? 'Single sign-on' : 'Users connect'
  }

  return { basic: 'Basic', passthrough: 'Gateway token' }[server.auth_type] || server.auth_type
}

export default function ManageServersPage() {
  return (
    <AdminOnly>
      <ManageServers />
    </AdminOnly>
  )
}

function ManageServers() {
  const { api } = useMcpGateway()
  const [modal, contextHolder] = Modal.useModal()

  // The server being edited, or null while adding one.
  const [editing, setEditing] = useState()

  const [reloads, setReloads] = useState(0)
  const load = () => setReloads(count => count + 1)

  const { loading, result: servers = [] } = useLatestResult(
    useCallback(() => api.getServers(), [api, reloads]),
    'servers'
  )

  const call = async (request, success) => {
    try {
      await request
      toast.success(success)
      load()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    }
  }

  const confirmDelete = server => {
    let validate = () => false
    modal.confirm({
      title: `Delete ${server.name}?`,
      icon: <ExclamationCircleFilled />,
      content: (
        <>
          <Paragraph>Users lose access to it, and the gateway forgets their connections to it.</Paragraph>
          <ConfirmInput
            name={server.service_id}
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
        call(api.deleteServer(server.service_id), `Deleted ${server.name}`)
      }
    })
  }

  const columns = [
    {
      title: 'Server',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (_, server) => (
        <>
          <div className='font-medium'>
            {server.icon} {server.name} <Text type='secondary'>({server.service_id})</Text>
          </div>
          <Text type='secondary' ellipsis={{ tooltip: server.mcp_url }}>
            {server.mcp_url}
          </Text>
        </>
      )
    },
    { title: 'Sign-in', key: 'signIn', width: 140, render: (_, server) => signIn(server) },
    {
      title: 'Access',
      key: 'access',
      width: 200,
      render: (_, server) => (
        <>
          <Tag>{server.requires_approval ? 'Approval required' : 'Open'}</Tag>
          {server.admin_only && <Tag color='purple'>Admins only</Tag>}
        </>
      )
    },
    {
      title: 'Status',
      key: 'status',
      width: 100,
      render: (_, server) => (server.is_active ? <Tag color='green'>Active</Tag> : <Tag color='default'>Inactive</Tag>)
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      render: (_, server) => {
        const activating = !server.is_active
        const action = activating ? 'Activate' : 'Deactivate'

        return (
          <Flex gap={4} align='center'>
            <Tooltip title='Edit'>
              <Button
                type='text'
                size='small'
                aria-label={`Edit ${server.name}`}
                icon={<Icons.Pencil className='size-4' />}
                onClick={() => setEditing(server)}
              />
            </Tooltip>
            <Tooltip title={action}>
              <Button
                type='text'
                size='small'
                aria-label={`${action} ${server.name}`}
                icon={activating ? <Icons.Power className='size-4' /> : <Icons.PowerOff className='size-4' />}
                onClick={() =>
                  call(api.updateServer(server.service_id, { is_active: activating }), `${action}d ${server.name}`)
                }
              />
            </Tooltip>
            <Tooltip title='Delete'>
              <Button
                type='text'
                size='small'
                aria-label={`Delete ${server.name}`}
                icon={<Icons.Trash2Icon className='size-4' />}
                onClick={() => confirmDelete(server)}
              />
            </Tooltip>
          </Flex>
        )
      }
    }
  ]

  return (
    <SectionContainer classProps='block overflow-y-auto'>
      {contextHolder}
      <Title level={2}>Manage Servers</Title>
      <Paragraph type='secondary'>MCP servers that users reach through the MCP gateway.</Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <Button type='primary' icon={<PlusOutlined />} onClick={() => setEditing(null)}>
          Add Server
        </Button>
      </Flex>
      <Spin spinning={loading}>
        <Table
          dataSource={servers.map(server => ({ ...server, key: server.service_id }))}
          columns={columns}
          tableLayout='fixed'
          pagination={{ position: ['bottomCenter'] }}
        />
      </Spin>
      {editing !== undefined && (
        <ServerDialog
          server={editing}
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
