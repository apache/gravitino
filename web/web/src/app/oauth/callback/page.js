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

import { useEffect } from 'react'
import { Box, Typography, CircularProgress } from '@mui/material'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

export default function OAuthCallback() {
  useEffect(() => {
    handleCallback()
  }, [])

  const handleCallback = async () => {
    try {
      // Get the OIDC provider and its UserManager
      const provider = await oauthProviderFactory.getProvider()
      const userManager = provider.getUserManager()

      if (!userManager) {
        throw new Error('OIDC UserManager not available. Please try logging in again.')
      }

      // Complete the signin process
      const user = await userManager.signinRedirectCallback()
      window.location.href = '/ui/metalakes'
    } catch (error) {
      console.error('OAuth callback failed:', error)

      // Clear any corrupted OIDC state from UserManager
      try {
        const provider = await oauthProviderFactory.getProvider()
        if (provider && provider.clearAuthData) {
          await provider.clearAuthData()
        }
      } catch (clearError) {
        console.error('Provider cleanup failed during error handling:', clearError)
      }

      // Redirect to login with error
      const errorMessage = encodeURIComponent(error.message || 'Authentication failed')
      window.location.href = `/ui/login?error=${errorMessage}`
    }
  }

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        gap: 2
      }}
    >
      <CircularProgress />
      <Typography variant='h6'>Processing authentication...</Typography>
      <Typography variant='body2' color='text.secondary'>
        Please wait while we complete your login.
      </Typography>
    </Box>
  )
}
