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
import { UserManager, WebStorageStateStore } from 'oidc-client-ts'
import { Box, Typography, CircularProgress } from '@mui/material'

export default function OAuthCallback() {
  useEffect(() => {
    handleCallback()
  }, [])

  const handleCallback = async () => {
    try {
      console.log('[OAuth Callback] Processing OIDC authentication callback')

      // Get the stored OIDC state to extract the original configuration
      const stateKeys = Object.keys(localStorage).filter(key => key.startsWith('oidc.'))
      console.log('[OAuth Callback] Found OIDC state keys:', stateKeys)

      // Find the signin state that contains the original configuration
      let storedState = null
      for (const key of stateKeys) {
        try {
          const stateData = JSON.parse(localStorage.getItem(key))
          if (stateData && stateData.authority) {
            storedState = stateData
            console.log('[OAuth Callback] Found signin state:', stateData)
            break
          }
        } catch (e) {
          // Skip invalid JSON
        }
      }

      if (!storedState) {
        throw new Error('No valid signin state found. Please try logging in again.')
      }

      // Create UserManager with the exact same configuration that was used during login
      const userManager = new UserManager({
        authority: storedState.authority,
        client_id: storedState.client_id,
        redirect_uri: storedState.redirect_uri,
        response_type: storedState.response_type || 'code',
        scope: storedState.scope || 'openid profile email',
        userStore: new WebStorageStateStore({ store: window.localStorage })
      })

      console.log('[OAuth Callback] Created UserManager with stored config:', {
        authority: storedState.authority,
        client_id: storedState.client_id,
        redirect_uri: storedState.redirect_uri
      })

      // Complete the signin process - this will use the stored state from localStorage
      const user = await userManager.signinRedirectCallback()
      console.log('[OAuth Callback] Authentication successful:', user)

      // The user is automatically stored by oidc-client-ts in localStorage
      // No need to manually store tokens

      // Redirect to the main application
      window.location.href = '/ui/metalakes'
    } catch (error) {
      console.error('[OAuth Callback] Error processing callback:', error)
      console.error('[OAuth Callback] Error details:', {
        message: error.message,
        stack: error.stack,
        name: error.name
      })

      // Clear any corrupted OIDC state
      localStorage.removeItem('oidc.user')
      Object.keys(localStorage).forEach(key => {
        if (key.startsWith('oidc.')) {
          console.log('[OAuth Callback] Removing corrupted state:', key)
          localStorage.removeItem(key)
        }
      })

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
