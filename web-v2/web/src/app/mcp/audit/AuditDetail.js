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

import { useEffect, useState } from 'react'
import { Descriptions, Drawer, Spin, Tag, Typography } from 'antd'
import toast from 'react-hot-toast'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { formatToDateTime } from '@/lib/utils/date'
import { gatewayDate } from '@/lib/utils/mcpAccess'
import { useMcpGateway } from '../McpGatewayProvider'

const { Text } = Typography

export const STATUS_COLORS = { success: 'green', error: 'red', unauthorized: 'orange', cancelled: 'default' }

// A successful call's response is kept only when the gateway runs with AUDIT_LOG_RESPONSES.
const RESPONSE_NOT_KEPT = 'Not kept. The gateway keeps responses only when AUDIT_LOG_RESPONSES is set.'

function Json({ value, empty = '-' }) {
  if (value === null || value === undefined || (typeof value === 'object' && !Object.keys(value).length)) {
    return <Text type='secondary'>{empty}</Text>
  }

  return (
    <pre className='m-0 max-h-80 overflow-auto whitespace-pre-wrap break-all rounded bg-gray-50 p-2 text-xs'>
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}

// One audit log entry, with what was sent and what came back.
export default function AuditDetail({ logId, onClose }) {
  const { api } = useMcpGateway()
  const [log, setLog] = useState()

  useEffect(() => {
    // Ignore an answer that comes after this entry closed, so it can't close another.
    let current = true
    api
      .getAuditLog(logId)
      .then(entry => current && setLog(entry))
      .catch(error => {
        if (!current) return
        toast.error(gatewayErrorMessage(error))
        onClose()
      })

    return () => {
      current = false
    }
  }, [api, logId, onClose])

  return (
    <Drawer open width={640} title={log ? `${log.service} · ${log.tool_name}` : 'Call'} onClose={onClose}>
      {!log ? (
        <Spin className='w-full' />
      ) : (
        <Descriptions column={1} size='small' bordered>
          <Descriptions.Item label='Time'>{formatToDateTime(gatewayDate(log.created_at))}</Descriptions.Item>
          <Descriptions.Item label='Account'>{log.username}</Descriptions.Item>
          <Descriptions.Item label='Server'>{log.service}</Descriptions.Item>
          <Descriptions.Item label='Tool'>{log.tool_name}</Descriptions.Item>
          <Descriptions.Item label='Method'>{log.method}</Descriptions.Item>
          <Descriptions.Item label='Status'>
            <Tag color={STATUS_COLORS[log.status]}>{log.status}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label='Duration'>
            {log.duration_ms === null ? '-' : `${log.duration_ms} ms`}
          </Descriptions.Item>
          {log.error_message && <Descriptions.Item label='Error'>{log.error_message}</Descriptions.Item>}
          <Descriptions.Item label='Arguments'>
            <Json value={log.request_params} />
          </Descriptions.Item>
          <Descriptions.Item label='Workload headers'>
            <Json value={log.request_headers} />
          </Descriptions.Item>
          <Descriptions.Item label='Response'>
            <Json value={log.response_data} empty={log.status === 'success' ? RESPONSE_NOT_KEPT : '-'} />
          </Descriptions.Item>
          <Descriptions.Item label='Client'>
            {[log.ip_address, log.user_agent].filter(Boolean).join(' · ') || '-'}
          </Descriptions.Item>
        </Descriptions>
      )}
    </Drawer>
  )
}
