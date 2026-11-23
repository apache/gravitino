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
import { Button, Flex, Input, Popconfirm, Spin, Table, Tag, Typography } from 'antd'
import toast from 'react-hot-toast'
import SectionContainer from '@/components/SectionContainer'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { latestRequest, mcpAccessStatus } from '@/lib/utils/mcpAccess'
import { useMcpGateway } from './McpGatewayProvider'
import useLatestResult from './useLatestResult'
import RequestAccessDialog from './RequestAccessDialog'

const { Title, Paragraph, Text } = Typography

const ACCESS = {
  granted: { color: 'green', label: 'Granted' },
  open: { color: 'green', label: 'Open' },
  pending: { color: 'gold', label: 'Pending approval' },
  denied: { color: 'red', label: 'Denied' },
  expired: { color: 'default', label: 'Expired' },
  none: { color: 'default', label: 'No access' }
}

// Servers the gateway signs users in to by itself; users connect to the others once.
const signsInAutomatically = server => server.auto_sso_enabled || ['basic', 'passthrough'].includes(server.auth_type)

export default function McpServersPage() {
  const { api } = useMcpGateway()
  const [search, setSearch] = useState('')
  const [requesting, setRequesting] = useState()
  const [reloads, setReloads] = useState(0)
  const load = () => setReloads(count => count + 1)

  const { loading, result: servers = [] } = useLatestResult(
    useCallback(async () => {
      const [catalog, permissions, requests] = await Promise.all([
        api.getCatalog(),
        api.getMyPermissions(),
        api.getMyRequests()
      ])

      return catalog.map(server => ({
        ...server,
        access: mcpAccessStatus(server, permissions, requests),
        note: latestRequest(server, requests)?.admin_notes
      }))
    }, [api, reloads]),
    'servers'
  )

  // The gateway runs the server's sign-in, then sends the browser back here.
  const connect = server => window.location.assign(api.connectUrl(server.service_id, window.location.href))

  const disconnect = async server => {
    try {
      await api.disconnect(server.service_id)
      toast.success(`Disconnected from ${server.name}`)
      load()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    }
  }

  const columns = [
    {
      title: 'Server',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (_, server) => (
        <>
          <div className='font-medium'>
            {server.icon} {server.name}
          </div>
          <Paragraph type='secondary' className='!mb-1' ellipsis={{ rows: 2, tooltip: server.description }}>
            {server.description}
          </Paragraph>
          <Text code copyable={{ tooltips: ['Copy the URL for MCP clients', 'Copied'] }}>
            {api.endpointUrl(server.service_id)}
          </Text>
        </>
      )
    },
    {
      title: 'Access',
      key: 'access',
      width: 240,
      render: (_, server) => (
        <>
          <Flex gap='small' align='center'>
            <Tag color={ACCESS[server.access].color}>{ACCESS[server.access].label}</Tag>
            {['none', 'denied', 'expired'].includes(server.access) && (
              <Button size='small' onClick={() => setRequesting(server)}>
                Request access
              </Button>
            )}
          </Flex>
          {server.access === 'denied' && server.note && <Text type='secondary'>{server.note}</Text>}
        </>
      )
    },
    {
      title: 'Sign-in',
      key: 'signIn',
      width: 220,
      render: (_, server) => {
        if (signsInAutomatically(server)) {
          return <Text type='secondary'>Automatic</Text>
        }
        if (!server.connected) {
          return (
            <Button size='small' onClick={() => connect(server)}>
              Connect
            </Button>
          )
        }
        if (server.token_expired) {
          return (
            <Flex gap='small' align='center'>
              <Tag>Expired</Tag>
              <Button size='small' onClick={() => connect(server)}>
                Reconnect
              </Button>
            </Flex>
          )
        }

        return (
          <Flex gap='small' align='center'>
            <Tag color='green'>Connected</Tag>
            <Popconfirm
              title={`Disconnect from ${server.name}?`}
              description='You will need to connect again to use it.'
              okText='Disconnect'
              okButtonProps={{ danger: true }}
              onConfirm={() => disconnect(server)}
            >
              <Button size='small' type='link' danger>
                Disconnect
              </Button>
            </Popconfirm>
          </Flex>
        )
      }
    }
  ]

  const visible = servers
    .filter(server => server.name.toLowerCase().includes(search.toLowerCase()))
    .map(server => ({ ...server, key: server.service_id }))

  return (
    <SectionContainer classProps='block overflow-y-auto'>
      <Title level={2}>My Servers</Title>
      <Paragraph type='secondary'>
        Point an MCP client, such as an IDE or an AI assistant, at a server&apos;s URL. The client signs you in with
        your Gravitino account, and the server acts as you.
      </Paragraph>
      <Flex justify='flex-end' className='mb-4'>
        <Input.Search
          className='w-1/3'
          placeholder='Search...'
          aria-label='Search the servers'
          onChange={e => setSearch(e.target.value)}
        />
      </Flex>
      <Spin spinning={loading}>
        <Table dataSource={visible} columns={columns} tableLayout='fixed' pagination={{ position: ['bottomCenter'] }} />
      </Spin>
      {requesting && (
        <RequestAccessDialog
          server={requesting}
          onClose={() => setRequesting(undefined)}
          onRequested={() => {
            setRequesting(undefined)
            load()
          }}
        />
      )}
    </SectionContainer>
  )
}
