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
import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { basicLoginAction, setIntervalIdAction, clearIntervalId } from '@/lib/store/auth'

const defaultValues = {
  username: '',
  password: ''
}

const schema = yup.object().shape({
  username: yup.string().required(),
  password: yup.string().required()
})

function BasicLogin() {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.auth)

  const {
    control,
    handleSubmit,
    reset,
    setError,
    formState: { errors }
  } = useForm({
    defaultValues: Object.assign({}, defaultValues),
    mode: 'onChange',
    resolver: yupResolver(schema)
  })

  useEffect(() => {
    if (store.intervalId) {
      dispatch(clearIntervalId())
    }
  }, [store.intervalId, dispatch])

  const onSubmit = async data => {
    try {
      // Using .unwrap() to catch failed login errors
      await dispatch(basicLoginAction({ username: data.username, password: data.password, router })).unwrap()

      // Clear the password after a successful login
      reset({ username: data.username, password: '' })
    } catch (error) {
      setError('password', {
        type: 'manual',
        message: error?.message || 'Login failed'
      })
    }
  }

  const onError = errors => {
    // Form validation errors are handled by the UI
  }

  return (
    <form autoComplete='off' onSubmit={handleSubmit(onSubmit, onError)}>
      <Form component={false} layout='vertical'>
        <Form.Item label='Username' validateStatus={errors.username ? 'error' : ''} help={errors.username?.message}>
          <Controller name='username' control={control} render={({ field }) => <Input {...field} placeholder='' />} />
        </Form.Item>

        <Form.Item label='Password' validateStatus={errors.password ? 'error' : ''} help={errors.password?.message}>
          <Controller
            name='password'
            control={control}
            render={({ field }) => <Input.Password {...field} placeholder='' />}
          />
        </Form.Item>

        <Button block size='large' type='primary' htmlType='submit'>
          Login
        </Button>
      </Form>
    </form>
  )
}

export default BasicLogin
