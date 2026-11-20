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

import { Menu } from 'antd'
import { usePathname, useRouter, useSearchParams } from 'next/navigation'
import { ROUTES } from '@/config/routes'
import { useMcpGateway } from './McpGatewayProvider'

export default function McpNav() {
  const router = useRouter()
  const pathname = usePathname()
  const currentMetalake = useSearchParams().get('metalake')
  const { me } = useMcpGateway()

  const items = [
    { key: ROUTES.Mcp, label: 'My Servers' },
    { key: ROUTES.McpAudit, label: 'Audit Log' },
    ...(me.is_admin
      ? [
          { key: ROUTES.McpServers, label: 'Manage Servers' },
          { key: ROUTES.McpAccessRequests, label: 'Access Requests' },
          { key: ROUTES.McpServiceAccounts, label: 'Service Accounts' }
        ]
      : [])
  ]

  const onClick = ({ key }) => {
    router.push(currentMetalake ? `${key}?metalake=${currentMetalake}` : key)
  }

  return (
    <Menu
      mode='inline'
      onClick={onClick}
      selectedKeys={[pathname]}
      style={{ width: 200 }}
      className='h-full'
      items={items}
    />
  )
}
