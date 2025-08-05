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

import { Box, Button, Typography, Alert } from '@mui/material'
import { useState, useEffect } from 'react'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

function OidcLogin() {
  const [isLoading, setIsLoading] = useState(true)
  const [userManager, setUserManager] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    const initializeOidc = async () => {
      try {
        // Get the provider directly - it handles config fetching internally
        const provider = await oauthProviderFactory.getProvider()

        if (provider.getType() !== 'oidc') {
          setError('OIDC provider not configured')
          setIsLoading(false)

          return
        }

        const sharedUserManager = provider.getUserManager()
        if (!sharedUserManager) {
          throw new Error('Failed to get UserManager from OIDC provider')
        }

        setUserManager(sharedUserManager)
        setIsLoading(false)
      } catch (error) {
        setError(error.message || 'Failed to initialize OIDC')
        setIsLoading(false)
      }
    }

    initializeOidc()
  }, [])

  if (isLoading) {
    return (
      <Box sx={{ textAlign: 'center', my: 4 }}>
        <Typography>Initializing authentication...</Typography>
      </Box>
    )
  }

  if (error) {
    return (
      <Box sx={{ textAlign: 'center', my: 4 }}>
        <Alert severity='error'>{error}</Alert>
      </Box>
    )
  }

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: 3,
        mt: 4
      }}
    >
      <OidcLoginButton userManager={userManager} />
    </Box>
  )
}

function OidcLoginButton({ userManager }) {
  const [isLoggingIn, setIsLoggingIn] = useState(false)

  const handleLogin = async () => {
    if (!userManager) {
      return
    }

    try {
      setIsLoggingIn(true)
      await userManager.signinRedirect()
    } catch (error) {
      setIsLoggingIn(false)
    }
  }

  return (
    <Button
      variant='contained'
      size='large'
      onClick={handleLogin}
      disabled={!userManager || isLoggingIn}
      sx={{ minWidth: 200, py: 1.5 }}
    >
      {isLoggingIn ? 'Redirecting...' : 'Sign In'}
    </Button>
  )
}

export default OidcLogin
