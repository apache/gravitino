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
import { Form, Input, Modal, Select } from 'antd'
import toast from 'react-hot-toast'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'
import { serviceAccountConfig, splitServiceAccountConfig } from '@/lib/utils/mcpServiceAccounts'
import { useMcpGateway } from '../McpGatewayProvider'

// Checks the config field holds a JSON object, so saving can't fail on it.
const jsonObjectRule = {
  validator: async (_, value) => {
    serviceAccountConfig('', value)
  }
}

export const PROVIDERS = [
  { value: 'google', label: 'Google Cloud (GKE, GCE)' },
  { value: 'aws', label: 'Amazon EKS' },
  { value: 'other', label: 'Other identity provider' }
]

// Registers a workload as a service account, or changes one (`account`).
export default function ServiceAccountDialog({ account, onClose, onSaved }) {
  const { api } = useMcpGateway()
  const [form] = Form.useForm()
  const [saving, setSaving] = useState(false)
  const [servers, setServers] = useState([])

  useEffect(() => {
    api
      .getServers()
      .then(list => setServers(list.map(server => ({ value: server.service_id, label: server.name }))))
      .catch(error => toast.error(gatewayErrorMessage(error)))
  }, [api])

  const { issuer, rest } = splitServiceAccountConfig(account?.additional_config)

  const submit = async () => {
    // The form shows what failed to validate.
    const values = await form.validateFields().catch(() => null)
    if (!values) return
    const config = serviceAccountConfig(values.issuer, values.config)
    setSaving(true)
    try {
      if (account) {
        await api.updateServiceAccount(account.id, {
          allowed_services: values.allowed_services,
          ...(config ? { additional_config: config } : { clear_additional_config: true })
        })
      } else {
        await api.createServiceAccount({
          name: values.name.trim(),
          team: values.team?.trim() || null,
          provider: values.provider,
          oauth_credentials: { service_account_name: values.identity.trim() },
          allowed_services: values.allowed_services,
          additional_config: config
        })
      }
      toast.success(`Saved ${account?.username || values.name.trim()}`)
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
      title={account ? `Edit ${account.username}` : 'Add Service Account'}
      okText='Save'
      confirmLoading={saving}
      onOk={submit}
      onCancel={onClose}
    >
      <Form
        form={form}
        layout='vertical'
        initialValues={{
          name: account?.username,
          team: account?.team,
          provider: account?.provider || 'google',
          identity: account?.service_account_name,
          allowed_services: account?.allowed_services || [],
          issuer,
          config: rest
        }}
      >
        <Form.Item
          name='name'
          label='Name'
          rules={[{ required: true, whitespace: true, min: 3, max: 100, transform: value => value?.trim() }]}
          extra='How the audit log names the account.'
        >
          <Input disabled={Boolean(account)} placeholder='svc-data-pipeline' />
        </Form.Item>
        <Form.Item name='team' label='Team'>
          <Input disabled={Boolean(account)} />
        </Form.Item>
        <Form.Item name='provider' label='Where the workload runs'>
          <Select disabled={Boolean(account)} options={PROVIDERS} />
        </Form.Item>
        <Form.Item
          name='identity'
          label='Workload identity'
          rules={[{ required: true, whitespace: true }]}
          extra='The email of the GCP service account, or on EKS namespace::serviceaccount::<cluster id>.'
        >
          <Input disabled={Boolean(account)} placeholder='etl@my-project.iam.gserviceaccount.com' />
        </Form.Item>
        <Form.Item
          name='allowed_services'
          label='Servers it may call'
          extra='Service accounts need no access requests; they reach only these servers.'
        >
          <Select mode='multiple' allowClear options={servers} />
        </Form.Item>
        <Form.Item
          name='issuer'
          label='Token issuer'
          extra='The issuer its tokens must come from. Needed for an EKS identity without a cluster id.'
        >
          <Input placeholder='https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLE' />
        </Form.Item>
        <Form.Item
          name='config'
          label='Other configuration (JSON)'
          rules={[jsonObjectRule]}
          extra='For example downstream_credentials, whose secret names must start with mcp-gateway-sa-.'
        >
          <Input.TextArea rows={4} className='font-mono' />
        </Form.Item>
      </Form>
    </Modal>
  )
}
