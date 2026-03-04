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

import { useRouter } from 'next/navigation'
import { useEffect } from 'react'
import { Button, Form, Input } from 'antd'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { loginAction, setIntervalIdAction, clearIntervalId, setAuthUser } from '@/lib/store/auth'

function DefaultLogin() {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.auth)
  const [form] = Form.useForm()

  useEffect(() => {
    if (store.intervalId) {
      clearIntervalId()
    }
  }, [store.intervalId])

  const onFinish = async values => {
    if (store.authType === 'simple' && store.anthEnable) {
      await dispatch(setAuthUser({ name: values.username, type: 'user' }))
      router.push('/metalakes')
    } else {
      await dispatch(loginAction({ params: values, router }))
      await dispatch(setIntervalIdAction())
    }
  }

  return (
    <Form
      form={form}
      layout='vertical'
      autoComplete='off'
      onFinish={onFinish}
      initialValues={{
        grant_type: 'client_credentials',
        client_id: '',
        client_secret: '',
        scope: ''
      }}
    >
      {store.authType === 'simple' && store.anthEnable ? (
        <>
          <Form.Item label='Username' name='username' rules={[{ required: true, message: 'Username is required' }]}>
            <Input placeholder='Please enter your username' />
          </Form.Item>
        </>
      ) : (
        <>
          <Form.Item
            label='Grant Type'
            name='grant_type'
            rules={[{ required: true, message: 'Grant Type is required' }]}
            className='mt-4'
          >
            <Input disabled placeholder='Please enter the grant type' />
          </Form.Item>

          <Form.Item
            label='Client ID'
            name='client_id'
            rules={[{ required: true, message: 'Client ID is required' }]}
            className='mt-4'
          >
            <Input placeholder='' />
          </Form.Item>

          <Form.Item
            label='Client Secret'
            name='client_secret'
            rules={[{ required: true, message: 'Client Secret is required' }]}
            className='mt-4'
          >
            <Input placeholder='' />
          </Form.Item>

          <Form.Item
            label='Scope'
            name='scope'
            rules={[{ required: true, message: 'Scope is required' }]}
            className='mt-4'
          >
            <Input placeholder='' />
          </Form.Item>
        </>
      )}

      <Form.Item className='mb-7 mt-12'>
        <Button type='primary' htmlType='submit' block size='large'>
          Login
        </Button>
      </Form.Item>
    </Form>
  )
}

export default DefaultLogin
