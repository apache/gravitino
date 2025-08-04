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

import { BaseOAuthProvider } from './base'

export class GenericOAuthProvider extends BaseOAuthProvider {
  constructor() {
    super()
    this.providerType = 'generic'
  }

  async initialize(config) {
    console.log('[GenericOAuthProvider] Initializing with config:', config)
    this.config = config
    console.log('[GenericOAuthProvider] Initialized successfully')
  }

  async getAccessToken() {
    // For generic OAuth providers, tokens are typically obtained through:
    // 1. Client credentials flow (backend handles token exchange)
    // 2. Authorization code flow (stored in localStorage after login)

    // Check localStorage for OIDC token first (from OidcLogin component)
    let token = typeof window !== 'undefined' ? localStorage.getItem('oidc_access_token') : null

    if (token) {
      console.log('[GenericOAuthProvider] OIDC token found in localStorage')

      return token
    }

    // Fallback to legacy accessToken for backward compatibility
    token = typeof window !== 'undefined' ? localStorage.getItem('accessToken') : null

    if (token) {
      console.log('[GenericOAuthProvider] Legacy token found in localStorage')

      return token
    }

    console.log('[GenericOAuthProvider] No token found in localStorage')

    return null
  }

  requiresWrapper() {
    // Generic providers don't need special React context wrappers

    return false
  }

  getWrapperComponent() {
    return null
  }
}
