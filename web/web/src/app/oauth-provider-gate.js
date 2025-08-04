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

import React, { useEffect, useState } from 'react'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

export default function OAuthProviderGate({ children }) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [wrapperComponent, setWrapperComponent] = useState(null)

  useEffect(() => {
    let mounted = true

    const initializeProvider = async () => {
      try {
        console.log('[OAuthProviderGate] Initializing OAuth provider...')

        const provider = await oauthProviderFactory.getProvider()

        if (!mounted) return

        console.log('[OAuthProviderGate] Provider initialized:', provider.getType())

        // Check if provider requires a wrapper component
        if (provider.requiresWrapper()) {
          const WrapperComponent = provider.getWrapperComponent()
          setWrapperComponent(() => WrapperComponent)
          console.log('[OAuthProviderGate] Provider requires wrapper component')
        } else {
          console.log('[OAuthProviderGate] Provider does not require wrapper component')
        }

        setLoading(false)
      } catch (err) {
        console.error('[OAuthProviderGate] Failed to initialize OAuth provider:', err)
        if (mounted) {
          setError(err)
          setLoading(false)
        }
      }
    }

    initializeProvider()

    return () => {
      mounted = false
    }
  }, [])

  if (loading) {
    if (error) {
      return <div style={{ color: 'red' }}>OAuth Provider initialization failed: {error.message || String(error)}</div>
    }

    return <div>Loading OAuth provider...</div>
  }

  // If provider requires a wrapper, use it
  if (wrapperComponent) {
    const WrapperComponent = wrapperComponent

    return <WrapperComponent>{children}</WrapperComponent>
  }

  // Otherwise, render children directly
  return <>{children}</>
}
