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
import { Form, Input, Modal } from 'antd'
import toast from 'react-hot-toast'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { useMcpGateway } from './McpGatewayProvider'

export default function RequestAccessDialog({ server, onClose, onRequested }) {
  const { api } = useMcpGateway()
  const [form] = Form.useForm()
  const [submitting, setSubmitting] = useState(false)

  const submit = async () => {
    const values = await form.validateFields().catch(() => null)
    if (!values) return
    setSubmitting(true)
    try {
      await api.requestAccess(server.id, values.justification)
      toast.success(`Requested access to ${server.name}`)
      onRequested()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal
      open
      title={`Request access to ${server.name}`}
      okText='Request'
      confirmLoading={submitting}
      onOk={submit}
      onCancel={onClose}
    >
      <Form form={form} layout='vertical'>
        <Form.Item
          name='justification'
          label='Why do you need access?'
          rules={[{ required: true, whitespace: true, message: 'Tell an admin why you need access' }]}
        >
          <Input.TextArea rows={4} maxLength={1000} />
        </Form.Item>
      </Form>
    </Modal>
  )
}
