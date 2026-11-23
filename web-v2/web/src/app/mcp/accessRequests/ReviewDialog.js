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

import { useState } from 'react'
import { Form, Input, InputNumber, Modal } from 'antd'
import toast from 'react-hot-toast'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { useMcpGateway } from '../McpGatewayProvider'

// Approves (`status` 'approved') or denies a request for access to an MCP server.
export default function ReviewDialog({ request, status, onClose, onReviewed }) {
  const { api } = useMcpGateway()
  const [form] = Form.useForm()
  const [saving, setSaving] = useState(false)
  const approving = status === 'approved'

  const submit = async () => {
    const values = await form.validateFields().catch(() => null)
    if (!values) return
    setSaving(true)
    try {
      await api.reviewRequest(request.id, { status, ...values })
      toast.success(`${approving ? 'Approved' : 'Denied'} access for ${request.username} to ${request.mcp_name}`)
      onReviewed()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      open
      title={`${approving ? 'Approve' : 'Deny'} access for ${request.username} to ${request.mcp_name}`}
      okText={approving ? 'Approve' : 'Deny'}
      okButtonProps={{ danger: !approving }}
      confirmLoading={saving}
      onOk={submit}
      onCancel={onClose}
    >
      <Form form={form} layout='vertical'>
        {approving && (
          <Form.Item
            name='expires_in_days'
            label='Expires after (days)'
            extra='Leave empty for access that does not expire.'
          >
            <InputNumber min={1} max={3650} className='w-full' />
          </Form.Item>
        )}
        <Form.Item name='admin_notes' label='Note to the user'>
          <Input.TextArea rows={3} maxLength={1000} />
        </Form.Item>
      </Form>
    </Modal>
  )
}
