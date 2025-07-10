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

import { useEffect, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { Box, CircularProgress, Typography, Alert } from '@mui/material'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { handleOAuthCallback } from '@/lib/store/auth'

const OAuthCallbackPage = () => {
  const router = useRouter()
  const searchParams = useSearchParams()
  const dispatch = useAppDispatch()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const processCallback = async () => {
      try {
        // Get token from URL parameters
        // The backend OAuth callback endpoint should redirect here with the token
        const access_token = searchParams.get('access_token') || searchParams.get('token')
        const refresh_token = searchParams.get('refresh_token')
        const error = searchParams.get('error')
        const errorDescription = searchParams.get('error_description')

        if (error) {
          setError(errorDescription || error)
          setLoading(false)

          return
        }

        if (!access_token) {
          setError('No authentication token received')
          setLoading(false)

          return
        }

        // Process the OAuth callback
        await dispatch(handleOAuthCallback({ access_token, refresh_token, router }))

        // Success - user will be redirected by the action
      } catch (err) {
        console.error('OAuth callback error:', err)
        setError(err.message || 'Authentication failed')
        setLoading(false)
      }
    }

    processCallback()
  }, [searchParams, dispatch, router])

  if (loading) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
          height: '100vh',
          gap: 2
        }}
      >
        <CircularProgress size={48} />
        <Typography variant='h6'>Completing your login...</Typography>
        <Typography variant='body2' color='text.secondary'>
          Please wait while we process your authentication.
        </Typography>
      </Box>
    )
  }

  if (error) {
    return (
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
          height: '100vh',
          gap: 2,
          p: 3
        }}
      >
        <Alert severity='error' sx={{ mb: 2 }}>
          <Typography variant='h6' gutterBottom>
            Authentication Failed
          </Typography>
          <Typography variant='body2'>{error}</Typography>
        </Alert>
        <Typography
          variant='body2'
          color='primary'
          sx={{ cursor: 'pointer', textDecoration: 'underline' }}
          onClick={() => router.push('/ui/login')}
        >
          Return to Login
        </Typography>
      </Box>
    )
  }

  return null
}

export default OAuthCallbackPage
