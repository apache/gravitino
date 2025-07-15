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

import Image from 'next/image'
import { Roboto } from 'next/font/google'
import { Box, Card, Grid, Button, CardContent, Typography } from '@mui/material'
import { useMsal, AuthenticatedTemplate, UnauthenticatedTemplate } from '@azure/msal-react'
import { getMsalConfig } from '@/lib/auth/msal'

import clsx from 'clsx'
import { useAuth } from '@/lib/provider/session'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

const LoginPage = () => {
  const { authError } = useAuth()

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
      scopes: getMsalConfig().auth.scope
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
