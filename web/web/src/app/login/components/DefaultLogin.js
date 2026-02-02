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
import { Grid, Button, Typography, TextField, FormControl, FormHelperText } from '@mui/material'
import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { loginAction, setIntervalIdAction, clearIntervalId } from '@/lib/store/auth'

const defaultValues = {
  grant_type: 'client_credentials',
  client_id: '',
  client_secret: '',
  scope: ''
}

const schema = yup.object().shape({
  grant_type: yup.string().required(),
  client_id: yup.string().required(),
  client_secret: yup.string().required(),
  scope: yup.string().required()
})

function DefaultLogin() {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.auth)

  const {
    control,
    handleSubmit,
    reset,
    formState: { errors }
  } = useForm({
    defaultValues: Object.assign({}, defaultValues),
    mode: 'onChange',
    resolver: yupResolver(schema)
  })

  useEffect(() => {
    if (store.intervalId) {
      clearIntervalId()
    }
  }, [store.intervalId])

  const onSubmit = async data => {
    await dispatch(loginAction({ params: data, router }))
    await dispatch(setIntervalIdAction())

    reset({ ...data })
  }

  const onError = errors => {
    // Form validation errors are handled by the UI
  }

  return (
    <form autoComplete='off' onSubmit={handleSubmit(onSubmit, onError)}>
      <Grid item xs={12} sx={{ mt: 4 }}>
        <FormControl fullWidth>
          <Controller
            name='grant_type'
            control={control}
            rules={{ required: true }}
            render={({ field: { value, onChange } }) => (
              <TextField
                value={value}
                label='Grant Type'
                disabled
                onChange={onChange}
                placeholder=''
                error={Boolean(errors.grant_type)}
              />
            )}
          />
          {errors.grant_type && (
            <FormHelperText className={'twc-text-error-main'}>{errors.grant_type.message}</FormHelperText>
          )}
        </FormControl>
      </Grid>

      <Grid item xs={12} sx={{ mt: 4 }}>
        <FormControl fullWidth>
          <Controller
            name='client_id'
            control={control}
            rules={{ required: true }}
            render={({ field: { value, onChange } }) => (
              <TextField
                value={value}
                label='Client ID'
                onChange={onChange}
                placeholder=''
                error={Boolean(errors.client_id)}
              />
            )}
          />
          {errors.client_id && (
            <FormHelperText className={'twc-text-error-main'}>{errors.client_id.message}</FormHelperText>
          )}
        </FormControl>
      </Grid>

      <Grid item xs={12} sx={{ mt: 4 }}>
        <FormControl fullWidth>
          <Controller
            name='client_secret'
            control={control}
            rules={{ required: true }}
            render={({ field: { value, onChange } }) => (
              <TextField
                value={value}
                label='Client Secret'
                onChange={onChange}
                placeholder=''
                error={Boolean(errors.client_secret)}
              />
            )}
          />
          {errors.client_secret && (
            <FormHelperText className={'twc-text-error-main'}>{errors.client_secret.message}</FormHelperText>
          )}
        </FormControl>
      </Grid>

      <Grid item xs={12} sx={{ mt: 4 }}>
        <FormControl fullWidth>
          <Controller
            name='scope'
            control={control}
            rules={{ required: true }}
            render={({ field: { value, onChange } }) => (
              <TextField value={value} label='Scope' onChange={onChange} placeholder='' error={Boolean(errors.scope)} />
            )}
          />
          {errors.scope && <FormHelperText className={'twc-text-error-main'}>{errors.scope.message}</FormHelperText>}
        </FormControl>
      </Grid>

      <Button fullWidth size='large' type='submit' variant='contained' sx={{ mb: 7, mt: 12 }}>
        Login
      </Button>
    </form>
  )
}

export default DefaultLogin
