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

import { Box, Button, Typography } from '@mui/material'
import { useState, useEffect, useCallback } from 'react'
import { UserManager, WebStorageStateStore } from 'oidc-client-ts'
import { useAuth } from '@/lib/provider/session'

function OidcLogin({ oauthConfig }) {
  const { authError } = useAuth()
  const [userManager, setUserManager] = useState(null)
  const [user, setUser] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [logoutMessage, setLogoutMessage] = useState('')

  useEffect(() => {
    // Check for logout/error messages in URL params
    const urlParams = new URLSearchParams(window.location.search)
    const message = urlParams.get('message')
    const error = urlParams.get('error')

    if (message === 'logged_out') {
      setLogoutMessage('You have been successfully logged out.')
    } else if (error) {
      setLogoutMessage(`Logout error: ${decodeURIComponent(error)}`)
    }
  }, [])

  const storeTokensForBackend = useCallback(user => {
    if (!user) return

    // For Azure AD, try to use ID token if access token is opaque
    // ID tokens are always JWTs and can be validated by the backend
    let tokenToStore = null
    let tokenType = 'access'

    if (user.access_token) {
      // Check if access token is a JWT (3 parts separated by dots)
      const isJWT = user.access_token.split('.').length === 3
      if (isJWT) {
        tokenToStore = user.access_token
        tokenType = 'access'
      } else if (user.id_token) {
        // Access token is opaque, use ID token instead
        tokenToStore = user.id_token
        tokenType = 'id'
        console.log('[OIDC] Using ID token because access token is opaque')
      } else {
        tokenToStore = user.access_token // Fallback to access token
        tokenType = 'access'
      }
    } else if (user.id_token) {
      tokenToStore = user.id_token
      tokenType = 'id'
    }

    if (tokenToStore) {
      localStorage.setItem('oidc_access_token', tokenToStore)
      localStorage.setItem('oidc_user_profile', JSON.stringify(user.profile))
      console.log(`[OIDC] Stored ${tokenType} token for backend authentication`)
    } else {
      console.warn('[OIDC] No suitable token available - API calls will fail')
    }
  }, [])

  const clearStoredTokens = useCallback(() => {
    localStorage.removeItem('oidc_access_token')
    localStorage.removeItem('oidc_user_profile')
  }, [])

  const initializeOidc = useCallback(async () => {
    try {
      if (!oauthConfig) {
        setIsLoading(false)

        return
      }

      // Create OIDC configuration
      const oidcConfig = {
        authority: oauthConfig.authority,
        client_id: oauthConfig.clientId,
        redirect_uri: `${window.location.origin}/ui/oauth/callback`,
        post_logout_redirect_uri: `${window.location.origin}/ui/oauth/logout`,
        response_type: 'code',
        scope: oauthConfig.scope || 'openid profile email',
        automaticSilentRenew: true,
        silent_redirect_uri: `${window.location.origin}/ui/oauth/silent-callback`,
        userStore: new WebStorageStateStore({ store: window.localStorage })
      }

      const manager = new UserManager(oidcConfig)
      setUserManager(manager)

      // Set up event handlers for automatic token management
      manager.events.addUserLoaded(user => {
        console.log('[OIDC] Token refresh successful - new tokens loaded')
        setUser(user)
        storeTokensForBackend(user)
      })

      manager.events.addUserUnloaded(() => {
        console.log('[OIDC] User session ended')
        setUser(null)
        clearStoredTokens()
      })

      manager.events.addAccessTokenExpired(() => {
        console.log('[OIDC] Access token expired - clearing session')
        setUser(null)
        clearStoredTokens()
      })

      manager.events.addSilentRenewError(error => {
        console.error('[OIDC] Silent token renewal failed:', error)
      })

      // Check if user is already signed in
      const currentUser = await manager.getUser()

      if (currentUser && !currentUser.expired) {
        setUser(currentUser)
        storeTokensForBackend(currentUser)
      } else if (currentUser && currentUser.expired) {
        // Try to refresh the token
        try {
          const refreshedUser = await manager.signinSilent()
          setUser(refreshedUser)
          storeTokensForBackend(refreshedUser)
        } catch (refreshError) {
          // Clear expired user
          await manager.removeUser()
        }
      }

      setIsLoading(false)
    } catch (error) {
      console.error('[OIDC Login] Initialization error:', error)
      setIsLoading(false)
    }
  }, [oauthConfig, storeTokensForBackend, clearStoredTokens])

  useEffect(() => {
    initializeOidc()
  }, [initializeOidc])

  if (isLoading) {
    return (
      <Box sx={{ textAlign: 'center', my: 4 }}>
        <Typography>Initializing authentication...</Typography>
      </Box>
    )
  }

  return (
    <>
      {/* Display logout message */}
      {logoutMessage && (
        <Box sx={{ mb: 2, p: 2, bgcolor: 'success.light', borderRadius: 1 }}>
          <Typography variant='body2' color='success.dark'>
            {logoutMessage}
          </Typography>
        </Box>
      )}

      {/* Auth error display */}
      {authError && (
        <Box sx={{ mb: 2, p: 2, bgcolor: 'error.light', borderRadius: 1 }}>
          <Typography variant='body2' color='error'>
            Authentication Error: {authError}
          </Typography>
        </Box>
      )}

      {user ? (
        <>
          <OidcProfile user={user} />
          <OidcLogoutButton userManager={userManager} />
        </>
      ) : (
        <OidcLoginButton userManager={userManager} providerName={oauthConfig.providerName} />
      )}
    </>
  )
}

function OidcLoginButton({ userManager, providerName = 'OAuth Provider' }) {
  const [isLoggingIn, setIsLoggingIn] = useState(false)

  const handleLogin = async () => {
    if (!userManager) {
      console.error('[OIDC Login] UserManager not initialized')

      return
    }

    try {
      setIsLoggingIn(true)
      await userManager.signinRedirect()
    } catch (error) {
      console.error('[OIDC Login] Login error:', error)
      setIsLoggingIn(false)
    }
  }

  return (
    <Button
      fullWidth
      size='large'
      variant='contained'
      onClick={handleLogin}
      disabled={isLoggingIn}
      sx={{ mb: 3, mt: 4 }}
    >
      {isLoggingIn ? 'Redirecting...' : `Login with ${providerName}`}
    </Button>
  )
}

function OidcLogoutButton({ userManager }) {
  const [isLoggingOut, setIsLoggingOut] = useState(false)

  const handleLogout = async () => {
    if (!userManager) {
      console.error('[OIDC Login] UserManager not initialized')

      return
    }

    try {
      setIsLoggingOut(true)
      await userManager.signoutRedirect()
    } catch (error) {
      console.error('[OIDC Login] Logout error:', error)
      setIsLoggingOut(false)
      alert('Logout failed. Please try again or close your browser.')
    }
  }

  return (
    <Button
      fullWidth
      size='large'
      variant='outlined'
      onClick={handleLogout}
      disabled={isLoggingOut}
      sx={{ mb: 3, mt: 4 }}
    >
      {isLoggingOut ? 'Logging out...' : 'Logout'}
    </Button>
  )
}

function OidcProfile({ user }) {
  const getDisplayName = () => {
    if (!user || !user.profile) return 'User'

    // Try different common claims for display name
    return (
      user.profile.name ||
      user.profile.preferred_username ||
      user.profile.email ||
      user.profile.unique_name ||
      user.profile.upn ||
      'User'
    )
  }

  return (
    <Typography variant='body2' sx={{ textAlign: 'center', my: 2 }}>
      Welcome, {getDisplayName()}
    </Typography>
  )
}

export default OidcLogin
