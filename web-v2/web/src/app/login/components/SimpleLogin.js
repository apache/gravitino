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
import { clearIntervalId, setAuthUser } from '@/lib/store/auth'

function SimpleLogin() {
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
    await dispatch(setAuthUser({ name: values.username, type: 'user' }))
    router.push('/metalakes')
  }

  return (
    <Form form={form} layout='vertical' autoComplete='off' onFinish={onFinish}>
      <Form.Item label='Username' name='username' rules={[{ required: true, message: 'Username is required' }]}>
        <Input placeholder='Please enter your username' />
      </Form.Item>

      <Form.Item className='mb-7 mt-12'>
        <Button type='primary' htmlType='submit' block size='large'>
          Login
        </Button>
      </Form.Item>
    </Form>
  )
}

export default SimpleLogin
