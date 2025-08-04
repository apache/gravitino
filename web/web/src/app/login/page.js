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
import { useEffect, useState } from 'react'
import { Box, Card, Grid, CardContent, Typography, Alert } from '@mui/material'
import clsx from 'clsx'

import OidcLogin from './components/OidcLogin'
import DefaultLogin from './components/DefaultLogin'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

// OAuth provider types
const OAUTH_PROVIDERS = {
  OIDC: 'oidc',
  GENERIC: 'generic'
}

const LoginPage = () => {
  const [oauthConfig, setOauthConfig] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [message, setMessage] = useState(null)

  useEffect(() => {
    // Check for URL parameters (logout messages, errors, etc.)
    const urlParams = new URLSearchParams(window.location.search)
    const messageParam = urlParams.get('message')
    const errorParam = urlParams.get('error')

    if (messageParam === 'logged_out') {
      setMessage('You have been successfully logged out.')
    } else if (errorParam) {
      setError(decodeURIComponent(errorParam))
    }

    // Fetch OAuth configuration from backend
    console.log('[LoginPage] Starting to fetch configs...')
    fetch('/configs')
      .then(response => {
        console.log('[LoginPage] Response status:', response.status)
        console.log('[LoginPage] Response headers:', response.headers)
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }

        return response.json()
      })
      .then(config => {
        console.log('[LoginPage] OAuth config received:', config)
        console.log('[LoginPage] Setting oauthConfig and stopping loading...')
        setOauthConfig(config)
        setLoading(false)
        console.log('[LoginPage] Loading state should now be false')
      })
      .catch(err => {
        console.error('[LoginPage] Failed to fetch OAuth config:', err)
        setError(`Failed to load authentication configuration: ${err.message}`)
        setLoading(false)
      })
  }, [])

  if (loading) {
    return (
      <Grid container spacing={2} sx={{ justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Box>
          <Card sx={{ width: 480 }}>
            <CardContent className='twc-p-12'>
              <Typography variant='body1' sx={{ textAlign: 'center' }}>
                Loading authentication...
              </Typography>
              <Typography variant='body2' sx={{ textAlign: 'center', mt: 2, color: 'text.secondary' }}>
                Fetching OAuth configuration from /configs
              </Typography>
              <Typography variant='body2' sx={{ textAlign: 'center', mt: 1, color: 'text.secondary' }}>
                Check browser console for details
              </Typography>
            </CardContent>
          </Card>
        </Box>
      </Grid>
    )
  }

  if (error) {
    return (
      <Grid container spacing={2} sx={{ justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Box>
          <Card sx={{ width: 480 }}>
            <CardContent className='twc-p-12'>
              <Alert severity='error'>{error}</Alert>
            </CardContent>
          </Card>
        </Box>
      </Grid>
    )
  }

  // Determine OAuth provider
  const provider = oauthConfig?.['gravitino.authenticator.oauth.provider']?.toLowerCase()
  console.log('[LoginPage] Determined provider:', provider)
  console.log('[LoginPage] About to render LoginContent with provider:', provider)

  return (
    <Grid container spacing={2} sx={{ justifyContent: 'center', alignItems: 'center', height: '100%' }}>
      <Box>
        <Card sx={{ width: 480 }}>
          <CardContent className='twc-p-12'>
            <Box className='twc-mb-8 twc-flex twc-items-center twc-justify-center'>
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

            {/* Display success messages */}
            {message && (
              <Box sx={{ mb: 2 }}>
                <Alert severity='success'>{message}</Alert>
              </Box>
            )}

            {/* Display error messages */}
            {error && (
              <Box sx={{ mb: 2 }}>
                <Alert severity='error'>{error}</Alert>
              </Box>
            )}

            <LoginContent provider={provider} oauthConfig={oauthConfig} />
          </CardContent>
        </Card>
      </Box>
    </Grid>
  )
}

// Main login content that switches based on OAuth configuration
function LoginContent({ provider, oauthConfig }) {
  console.log('[LoginContent] Rendering with provider:', provider)
  console.log('[LoginContent] OAuth config available:', !!oauthConfig)

  // Use OIDC for any provider that's not 'generic' (same logic as factory)
  const configuredProvider = oauthConfig?.['gravitino.authenticator.oauth.provider']
  const authority = oauthConfig?.['gravitino.authenticator.oauth.authority']
  const clientId = oauthConfig?.['gravitino.authenticator.oauth.client-id']

  if (configuredProvider && configuredProvider.toLowerCase() !== 'generic' && authority && clientId) {
    console.log('[LoginContent] OIDC provider detected, rendering OidcLogin component')

    // Create OIDC config from backend config - use existing provider field for display name
    const providerName = getProviderDisplayName(configuredProvider)

    const oidcConfig = {
      authority: authority,
      clientId: clientId,
      scope: oauthConfig['gravitino.authenticator.oauth.scope'] || 'openid profile email',
      providerName: providerName
    }

    return <OidcLogin oauthConfig={oidcConfig} />
  } else {
    console.log('[LoginContent] Using DefaultLogin component (generic/backward compatibility)')

    return <DefaultLogin oauthConfig={oauthConfig} />
  }
}

// Helper function to get display name for provider
function getProviderDisplayName(provider) {
  if (!provider) return 'OAuth Provider'

  const displayNames = {
    azure: 'Microsoft',
    microsoft: 'Microsoft',
    google: 'Google',
    auth0: 'Auth0',
    keycloak: 'Keycloak',
    okta: 'Okta',
    oidc: 'OAuth Provider'
  }

  return displayNames[provider.toLowerCase()] || provider.charAt(0).toUpperCase() + provider.slice(1)
}

export default LoginPage
