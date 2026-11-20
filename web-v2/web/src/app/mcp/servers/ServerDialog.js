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
import { Checkbox, Form, Input, Modal, Select, Switch } from 'antd'
import toast from 'react-hot-toast'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { hasBasicAuth, serverFields } from '@/lib/utils/mcpServers'
import { useMcpGateway } from '../McpGatewayProvider'

const CATEGORIES = ['data', 'ai', 'analytics', 'communication', 'devops', 'other']

const AUTH_TYPES = [
  { value: 'oauth', label: 'OAuth' },
  { value: 'basic', label: "Basic, with the gateway's credentials" },
  { value: 'passthrough', label: 'A token from the gateway' }
]

const SERVICE_AUTH_TYPES = [
  { value: 'passthrough', label: 'A token from the gateway' },
  { value: 'basic', label: "Basic, with the gateway's credentials" },
  { value: 'credential_forward', label: "The service account's own credentials" }
]

const NEW_SERVER = {
  icon: '🔌',
  category: 'data',
  auth_type: 'oauth',
  auto_sso_enabled: true,
  service_auth_type: 'passthrough',
  requires_approval: true,
  admin_only: false,
  is_active: true
}

// An http(s) URL with a host, as the gateway checks it; service names such as
// http://mcp-server:8000/mcp count.
const httpUrl = (_, value) => {
  if (!value) return Promise.resolve()
  try {
    const url = new URL(value)
    if (['http:', 'https:'].includes(url.protocol) && url.hostname && !url.username && !url.password) {
      return Promise.resolve()
    }
  } catch {
    // Not a URL at all.
  }

  return Promise.reject(new Error('Enter an http or https URL'))
}

// Registers an MCP server with the gateway, or changes one (`server`).
export default function ServerDialog({ server, onClose, onSaved }) {
  const { api } = useMcpGateway()
  const [form] = Form.useForm()
  const [saving, setSaving] = useState(false)
  const authType = Form.useWatch('auth_type', form)
  const singleSignOn = Form.useWatch('auto_sso_enabled', form)
  const serviceAuthType = Form.useWatch('service_auth_type', form)
  const removeClientSecret = Form.useWatch('remove_oauth_client_secret', form)

  // The same credentials serve people's and service accounts' basic auth.
  const usesBasic = authType === 'basic' || serviceAuthType === 'basic'

  const submit = async () => {
    // The form shows what failed to validate.
    const values = await form.validateFields().catch(() => null)
    if (!values) return

    const fields = serverFields(values)
    setSaving(true)
    try {
      if (server) {
        const changes = Object.fromEntries(Object.entries(fields).filter(([key]) => key !== 'service_id'))
        await api.updateServer(server.service_id, changes)
      } else {
        await api.createServer(fields)
      }
      toast.success(`Saved ${values.name}`)
      onSaved()
    } catch (error) {
      toast.error(gatewayErrorMessage(error))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      open
      width={640}
      title={server ? `Edit ${server.name}` : 'Add MCP Server'}
      okText='Save'
      confirmLoading={saving}
      onOk={submit}
      onCancel={onClose}
    >
      <Form form={form} layout='vertical' initialValues={server || NEW_SERVER}>
        <Form.Item
          name='service_id'
          label='Server ID'
          extra='Part of the URL that MCP clients use; it cannot change later.'
          rules={[
            { required: true, min: 3, max: 100 },
            {
              pattern: /^[a-z0-9][a-z0-9_.-]*$/,
              message: "Use lower-case letters, digits, '.', '_' and '-', starting with a letter or digit"
            }
          ]}
        >
          <Input disabled={!!server} />
        </Form.Item>
        <Form.Item name='name' label='Name' rules={[{ required: true, min: 3, max: 255 }]}>
          <Input />
        </Form.Item>
        <Form.Item name='description' label='Description'>
          <Input.TextArea rows={2} />
        </Form.Item>
        <div className='flex gap-4'>
          <Form.Item name='icon' label='Icon' className='w-1/3'>
            <Input maxLength={50} />
          </Form.Item>
          <Form.Item name='category' label='Category' className='grow'>
            <Select options={CATEGORIES.map(value => ({ value, label: value }))} />
          </Form.Item>
        </div>
        <Form.Item
          name='mcp_url'
          label='MCP server URL'
          rules={[{ required: true, message: 'Enter the MCP server URL' }, { validator: httpUrl }]}
        >
          <Input placeholder='http://mcp-server:8000/mcp' />
        </Form.Item>
        <Form.Item name='auth_type' label='How the gateway signs in for users'>
          <Select options={AUTH_TYPES} />
        </Form.Item>
        {authType === 'oauth' && (
          <Form.Item
            name='auto_sso_enabled'
            label='Single sign-on'
            valuePropName='checked'
            extra="On: the gateway forwards or exchanges the user's own token. Off: each user connects to the server's own sign-in."
          >
            <Switch />
          </Form.Item>
        )}
        {authType === 'oauth' && singleSignOn && (
          <>
            <Form.Item
              name='audience'
              preserve={false}
              label='Audience'
              extra="The audience of the tokens the server accepts. Users' tokens are forwarded for the gateway's own audience and exchanged for any other."
              rules={[{ required: true }]}
            >
              <Input />
            </Form.Item>
          </>
        )}
        {authType === 'oauth' && !singleSignOn && (
          <>
            <Form.Item
              name='oauth_metadata_url'
              label='Authorization server metadata URL'
              dependencies={['oauth_client_secret', 'remove_oauth_client_secret']}
              extra='Needed with a client secret, which is sent only to this authorization server.'
              rules={[
                ({ getFieldValue }) => ({
                  required: Boolean(
                    getFieldValue('oauth_client_secret') ||
                    (server?.oauth_client_secret_configured && !getFieldValue('remove_oauth_client_secret'))
                  ),
                  message: 'A client secret needs the authorization server metadata URL'
                }),
                { validator: httpUrl }
              ]}
            >
              <Input placeholder='Found from the server URL when empty' />
            </Form.Item>
            <Form.Item name='oauth_client_id' label='Client ID' preserve={false}>
              <Input placeholder='Registered by the gateway when empty' />
            </Form.Item>
            <Form.Item name='oauth_client_secret' label='Client secret'>
              <Input.Password placeholder={server ? 'Unchanged' : 'Optional'} disabled={removeClientSecret} />
            </Form.Item>
            {server?.oauth_client_secret_configured && (
              <Form.Item name='remove_oauth_client_secret' valuePropName='checked' preserve={false}>
                <Checkbox>Remove the client secret, for a public client</Checkbox>
              </Form.Item>
            )}
          </>
        )}
        {authType === 'oauth' && (
          <Form.Item name='oauth_scopes' label='Scopes'>
            <Input placeholder='Optional' />
          </Form.Item>
        )}
        <Form.Item name='service_auth_type' label='How the gateway signs in for service accounts'>
          <Select options={SERVICE_AUTH_TYPES} />
        </Form.Item>
        {usesBasic && (
          <div className='flex gap-4'>
            <Form.Item name='basic_auth_username' label='User name' className='grow' rules={[{ required: true }]}>
              <Input />
            </Form.Item>
            <Form.Item
              name='basic_auth_password'
              label='Password'
              className='grow'
              rules={[{ required: !hasBasicAuth(server) }]}
            >
              <Input.Password placeholder={hasBasicAuth(server) ? 'Unchanged' : ''} />
            </Form.Item>
          </div>
        )}
        <div className='flex gap-8'>
          <Form.Item name='requires_approval' label='Requires approval' valuePropName='checked'>
            <Switch />
          </Form.Item>
          <Form.Item name='admin_only' label='Only admins see it' valuePropName='checked'>
            <Switch />
          </Form.Item>
          <Form.Item name='is_active' label='Active' valuePropName='checked'>
            <Switch />
          </Form.Item>
        </div>
      </Form>
    </Modal>
  )
}
