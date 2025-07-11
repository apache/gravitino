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
import { Box, Card, Grid, Button, CardContent, Typography } from '@mui/material'
import { useMsal, AuthenticatedTemplate, UnauthenticatedTemplate } from '@azure/msal-react'

import clsx from 'clsx'

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

const LoginPage = () => {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.auth)
  const { authError } = useAuth()
  const [oauthConfig, setOauthConfig] = useState(null)
  const [configLoading, setConfigLoading] = useState(true)
  const [configError, setConfigError] = useState(null)
  const fetchingRef = useRef(false)

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
  }, [dispatch]) // Run only once on mount, dispatch is a safe dependency

  const onSubmit = async data => {
    await dispatch(loginAction({ params: data, router }))
    await dispatch(setIntervalIdAction())
  }

  const onError = errors => {
    console.error('fields error', errors)
  }

  const handleOAuthLogin = async () => {
    try {
      await dispatch(initiateOAuthFlow())
    } catch (error) {
      console.error('OAuth login failed:', error)
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

            <AuthenticatedTemplate>
              <Profile />
              <LogoutButton />
            </AuthenticatedTemplate>
            <UnauthenticatedTemplate>
              <LoginButton />
            </UnauthenticatedTemplate>
          </CardContent>
        </Card>
      </Box>
    </Grid>
  )
}

function LoginButton() {
  const { instance } = useMsal()

  const handleLogin = () => {
    instance.loginRedirect({
      scopes: ['openid', 'profile', 'email', 'offline_access', 'User.Read']
    })
  }

  return (
    <Button fullWidth size='large' variant='contained' onClick={handleLogin} sx={{ mb: 3, mt: 4 }}>
      Login with Microsoft
    </Button>
  )
}

function LogoutButton() {
  const { instance } = useMsal()

  return (
    <Button fullWidth size='large' variant='outlined' onClick={() => instance.logoutRedirect()} sx={{ mb: 3, mt: 4 }}>
      Logout
    </Button>
  )
}

function Profile() {
  const { accounts } = useMsal()
  const account = accounts[0]

  return account ? (
    <Typography variant='body2' sx={{ textAlign: 'center', my: 2 }}>
      Welcome, {account.username}
    </Typography>
  ) : null
}

export default LoginPage
