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

export default function OAuthLogout() {
  useEffect(() => {
    const handleLogoutCallback = async () => {
      try {
        // Create UserManager for logout callback processing
        const userManager = new UserManager({
          userStore: new WebStorageStateStore({ store: window.localStorage })
        })

        // Complete the signout process and clean up OIDC library state
        await userManager.signoutRedirectCallback()
        await userManager.removeUser()

        // Clear all authentication data
        clearAllAuthenticationData()

        // Redirect to login with success message
        window.location.href = '/ui/login?message=logged_out'
      } catch (error) {
        console.error('[OAuth Logout] Error processing logout callback:', error)

        // Even if there's an error, clear all auth data and redirect
        try {
          const userManager = new UserManager({
            userStore: new WebStorageStateStore({ store: window.localStorage })
          })
          await userManager.removeUser()
        } catch (removeError) {
          // Ignore removeUser errors
        }

        clearAllAuthenticationData()
        window.location.href = '/ui/login?message=logged_out'
      }
    }
    handleLogoutCallback()
  }, [])

  const clearAllAuthenticationData = () => {
    // Clear OIDC tokens
    localStorage.removeItem('oidc_access_token')
    localStorage.removeItem('oidc_user_profile')

    // Clear legacy authentication tokens
    localStorage.removeItem('accessToken')
    localStorage.removeItem('expiredIn')
    localStorage.removeItem('isIdle')
    localStorage.removeItem('refresh_token')
    localStorage.removeItem('authParams')
    localStorage.removeItem('oauthUrl')
    localStorage.removeItem('oauthProvider')
    localStorage.removeItem('version')

    // Clear all OIDC UserManager keys
    const keysToRemove = []
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i)
      if (key && key.startsWith('oidc.')) {
        keysToRemove.push(key)
      }
    }
    keysToRemove.forEach(key => localStorage.removeItem(key))
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
      <Typography variant='h6'>Logging out...</Typography>
      <Typography variant='body2' color='text.secondary'>
        Please wait while we complete your logout.
      </Typography>
    </Box>
  )
}
