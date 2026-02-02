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
import { useState, useEffect } from 'react'

import { Box, IconButton } from '@mui/material'

import Icon from '@/components/Icon'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { logoutAction } from '@/lib/store/auth'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

const LogoutButton = () => {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const authStore = useAppSelector(state => state.auth)
  const [showLogoutButton, setShowLogoutButton] = useState(false)

  useEffect(() => {
    const checkAuthStatus = async () => {
      try {
        // Check Redux store first
        if (authStore.authToken) {
          setShowLogoutButton(true)

          return
        }

        // Check provider authentication status for OIDC
        const provider = await oauthProviderFactory.getProvider()
        if (provider) {
          const isAuth = await provider.isAuthenticated()
          setShowLogoutButton(isAuth)
        } else {
          setShowLogoutButton(false)
        }
      } catch (error) {
        // If provider check fails, fallback to false
        setShowLogoutButton(false)
      }
    }

    checkAuthStatus()
  }, [authStore.authToken])

  const handleLogout = () => {
    dispatch(logoutAction({ router }))
  }

  return (
    <Box>
      {showLogoutButton ? (
        <IconButton onClick={handleLogout}>
          <Icon icon={'bx:exit'} />
        </IconButton>
      ) : null}
    </Box>
  )
}

export default LogoutButton
