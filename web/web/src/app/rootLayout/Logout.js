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

import { Box, IconButton } from '@mui/material'
import { UserManager, WebStorageStateStore } from 'oidc-client-ts'

import Icon from '@/components/Icon'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { clearIntervalId, setAuthToken } from '@/lib/store/auth'

const LogoutButton = () => {
  const dispatch = useAppDispatch()
  const authStore = useAppSelector(state => state.auth)

  const handleLogout = async () => {
    try {
      // Clear UserManager's user data to prevent token restoration
      const userManager = new UserManager({
        userStore: new WebStorageStateStore({ store: window.localStorage })
      })
      await userManager.removeUser()
    } catch (error) {
      // Continue with logout even if UserManager cleanup fails
    }

    // Clear all authentication data
    localStorage.removeItem('oidc_access_token')
    localStorage.removeItem('oidc_user_profile')
    localStorage.removeItem('accessToken')
    localStorage.removeItem('authParams')
    localStorage.removeItem('expiredIn')
    localStorage.removeItem('isIdle')
    localStorage.removeItem('refresh_token')
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

    // Clear Redux auth state
    dispatch(clearIntervalId())
    dispatch(setAuthToken(''))

    // Redirect to login
    window.location.href = '/ui/login?message=logged_out'
  }

  // Show logout button if any authentication data exists
  const showLogoutButton =
    authStore.authToken ||
    (typeof window !== 'undefined' &&
      (localStorage.getItem('oidc_access_token') || localStorage.getItem('accessToken')))

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
