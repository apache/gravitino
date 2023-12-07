/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect } from 'react'

import { useRouter, usePathname } from 'next/navigation'

import { Box, Card, Grid, Button, CardContent, Typography, TextField, FormControl, FormHelperText } from '@mui/material'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'
import { useAuth } from '@/lib/provider/session'
import { useLocalStorage } from 'react-use'

import { useAppSelector } from '@/lib/hooks/useStore'

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

const LoginPage = () => {
  const auth = useAuth()
  const router = useRouter()
  const pathname = usePathname()

  const [version] = useLocalStorage('version')

  const authStore = useAppSelector(state => state.auth)

  const {
    control,
    handleSubmit,
    formState: { errors }
  } = useForm({
    defaultValues,
    mode: 'onChange',
    resolver: yupResolver(schema)
  })

  const onSubmit = data => {
    auth.login(data)
  }

  useEffect(() => {
    if (authStore.authType === 'simple') {
      router.push('/')
    } else {
      const check = version && pathname === '/login'
      if (check) {
        router.push('/')
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <Grid container spacing={2} sx={{ justifyContent: 'center', alignItems: 'center', height: '100%' }}>
      <Box>
        <Card sx={{ width: 480 }}>
          <CardContent className={`twc-p-12`}>
            <Box className={`twc-mb-8 twc-flex twc-items-center twc-justify-center`}>
              <Typography variant='h6' className={`twc-font-bold twc-text-[1.5rem] logoText`}>
                Gravitino
              </Typography>
            </Box>

            <form autoComplete='off' onSubmit={handleSubmit(onSubmit)}>
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
                      <TextField
                        value={value}
                        label='Scope'
                        onChange={onChange}
                        placeholder=''
                        error={Boolean(errors.scope)}
                      />
                    )}
                  />
                  {errors.scope && (
                    <FormHelperText className={'twc-text-error-main'}>{errors.scope.message}</FormHelperText>
                  )}
                </FormControl>
              </Grid>

              <Button fullWidth size='large' type='submit' variant='contained' sx={{ mb: 7, mt: 12 }}>
                Login
              </Button>
            </form>
          </CardContent>
        </Card>
      </Box>
    </Grid>
  )
}

export default LoginPage
