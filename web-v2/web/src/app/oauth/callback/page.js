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
import { Flex, Spin, Typography } from 'antd'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

const { Title, Text } = Typography

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
      window.location.href = process.env.NEXT_PUBLIC_BASE_PATH + '/metalakes'
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
      window.location.href = `${process.env.NEXT_PUBLIC_BASE_PATH}/login?error=${errorMessage}`
    }
  }

  return (
    <Flex vertical align='center' justify='center' gap={16} style={{ minHeight: '100vh' }}>
      <Spin size='large' />
      <Title level={4} style={{ margin: 0 }}>
        Processing authentication...
      </Title>
      <Text type='secondary'>Please wait while we complete your login.</Text>
    </Flex>
  )
}
