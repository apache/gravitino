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
import Image from 'next/image'
import { Roboto } from 'next/font/google'
import { useEffect, useState, useRef } from 'react'
import { Box, Card, Grid, Button, CardContent, Typography, TextField, FormControl, FormHelperText } from '@mui/material'

import clsx from 'clsx'
import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { useAuth } from '@/lib/provider/session'
import {
  loginAction,
  setIntervalIdAction,
  clearIntervalId,
  getAuthConfigs,
  getOAuthConfig,
  initiateOAuthFlow
} from '@/lib/store/auth'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

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
  const router = useRouter()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.auth)
  const { authError } = useAuth()
  const [oauthConfig, setOauthConfig] = useState(null)
  const [configLoading, setConfigLoading] = useState(true)
  const [configError, setConfigError] = useState(null)
  const fetchingRef = useRef(false)

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
    // Prevent multiple simultaneous fetches
    if (fetchingRef.current) {
      return
    }

    fetchingRef.current = true
    setConfigLoading(true)
    setConfigError(null)

    // Fetch auth configuration to determine login method
    dispatch(getAuthConfigs())
      .then(result => {
        if (result.payload) {
          // Also fetch OAuth-specific configuration
          dispatch(getOAuthConfig())
            .then(oauthResult => {
              if (oauthResult.payload) {
                setOauthConfig(oauthResult.payload)
              } else {
                setOauthConfig({ authorizationCodeFlowEnabled: false })
              }
              setConfigLoading(false)
              fetchingRef.current = false
            })
            .catch(error => {
              // OAuth not configured, continue with regular auth
              setOauthConfig({ authorizationCodeFlowEnabled: false })
              setConfigLoading(false)
              fetchingRef.current = false
            })
        } else {
          setConfigError('Failed to load authentication configuration')
          setConfigLoading(false)
          fetchingRef.current = false
        }
      })
      .catch(error => {
        setConfigError('Failed to load authentication configuration: ' + error.message)
        setOauthConfig({ authorizationCodeFlowEnabled: false })
        setConfigLoading(false)
        fetchingRef.current = false
      })

    // Cleanup function
    return () => {
      fetchingRef.current = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // Run only once on mount

  const onSubmit = async data => {
    await dispatch(loginAction({ params: data, router }))
    await dispatch(setIntervalIdAction())

    reset({ ...data })
  }

  const onError = errors => {
    console.error('fields error', errors)
  }

  const handleOAuthLogin = async () => {
    try {
      // Get current URL to redirect back to after OAuth
      const currentUrl = window.location.origin
      const redirectUri = `${currentUrl}/oauth/callback`

      // Initiate OAuth flow
      await dispatch(initiateOAuthFlow(redirectUri))
    } catch (error) {
      console.error('OAuth login failed:', error)

      // Could show error toast here
    }
  }

  return (
    <Grid container spacing={2} sx={{ justifyContent: 'center', alignItems: 'center', height: '100%' }}>
      <Box>
        <Card sx={{ width: 480 }}>
          <CardContent className={`twc-p-12`}>
            <Box className={`twc-mb-8 twc-flex twc-items-center twc-justify-center`}>
              <Image
                src={`${process.env.NEXT_PUBLIC_BASE_PATH ?? ''}/icons/gravitino.svg`}
                width={24}
                height={24}
                alt='logo'
              />
              <Typography variant='h6' className={clsx('twc-text-[black] twc-ml-2 twc-text-[1.5rem]', fonts.className)}>
                Gravitino
              </Typography>
            </Box>

            {/* Auth error display */}
            {authError && (
              <Box sx={{ mb: 2, p: 2, bgcolor: 'error.light', borderRadius: 1 }}>
                <Typography variant='body2' color='error'>
                  Authentication Error: {authError}
                </Typography>
              </Box>
            )}

            {/* Show OAuth redirect button if authorization code flow is enabled */}
            {oauthConfig?.authorizationCodeFlowEnabled ? (
              <>
                <Button
                  fullWidth
                  size='large'
                  variant='contained'
                  onClick={handleOAuthLogin}
                  disabled={store.oauthRedirecting}
                  sx={{ mb: 3, mt: 4 }}
                >
                  {store.oauthRedirecting
                    ? 'Redirecting...'
                    : `Login with ${oauthConfig?.providerName || 'OAuth Provider'}`}
                </Button>

                <Typography variant='body2' sx={{ textAlign: 'center', my: 2 }}>
                  Or use client credentials:
                </Typography>
              </>
            ) : null}

            {/* Client credentials form */}
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
