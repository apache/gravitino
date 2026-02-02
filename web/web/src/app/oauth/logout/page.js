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
import { useRouter } from 'next/navigation'
import { Box, Typography, CircularProgress } from '@mui/material'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { logoutAction } from '@/lib/store/auth'

export default function OAuthLogout() {
  const router = useRouter()
  const dispatch = useAppDispatch()

  useEffect(() => {
    const handleLogoutCallback = async () => {
      try {
        // Get the OIDC provider for logout callback processing
        const provider = await oauthProviderFactory.getProvider()

        if (provider && provider.getUserManager) {
          const userManager = provider.getUserManager()

          if (userManager) {
            // Complete the OIDC signout redirect callback
            await userManager.signoutRedirectCallback()
          }
        }
      } catch (error) {
        console.warn('OIDC logout callback failed:', error)
      } finally {
        // Use centralized logout action for complete cleanup
        dispatch(logoutAction({ router }))
      }
    }

    handleLogoutCallback()
  }, [dispatch, router])

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
