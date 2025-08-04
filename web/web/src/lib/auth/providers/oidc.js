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

export class OidcOAuthProvider extends BaseOAuthProvider {
  constructor() {
    super()
    this.providerType = 'oidc'
    this.oidcConfig = null
  }

  async initialize(config) {
    this.config = config

    const authority = config['gravitino.authenticator.oauth.authority']
    const clientId = config['gravitino.authenticator.oauth.client-id']
    const jwksUri = config['gravitino.authenticator.oauth.jwks-uri']
    const scope = config['gravitino.authenticator.oauth.scope'] || 'openid profile email'
    const provider = config['gravitino.authenticator.oauth.provider'] || 'OAuth Provider'

    if (!authority || !clientId) {
      throw new Error('OIDC provider requires both authority and client-id to be configured')
    }

    // Map provider names to display names
    const providerDisplayNames = {
      azure: 'Microsoft',
      google: 'Google',
      auth0: 'Auth0',
      keycloak: 'Keycloak',
      okta: 'Okta'
    }

    this.oidcConfig = {
      authority: authority,
      clientId: clientId,
      scope: scope,
      providerName: providerDisplayNames[provider.toLowerCase()] || provider,
      jwksUri: jwksUri
    }
  }

  async getAccessToken() {
    const token = typeof window !== 'undefined' ? localStorage.getItem('oidc_access_token') : null

    console.log('[OidcOAuthProvider] getAccessToken called')
    console.log('[OidcOAuthProvider] Token found in localStorage:', !!token)
    console.log('[OidcOAuthProvider] Token value (first 20 chars):', token ? token.substring(0, 20) + '...' : 'null')

    // Just return the token if it exists - the OIDC library already validated it
    if (token && token.trim().length > 0) {
      console.log('[OidcOAuthProvider] Token exists and is non-empty, returning token')

      return token
    }

    console.log('[OidcOAuthProvider] No token found, returning null')

    return null
  }

  getOidcConfig() {
    return this.oidcConfig
  }

  requiresWrapper() {
    return false
  }

  getWrapperComponent() {
    return null
  }

  /**
   * Check if user is currently authenticated
   */
  isAuthenticated() {
    if (typeof window === 'undefined') return false

    const token = localStorage.getItem('oidc_access_token')
    const userProfile = localStorage.getItem('oidc_user_profile')

    return !!(token && userProfile)
  }

  /**
   * Get current user profile
   */
  getUserProfile() {
    if (typeof window === 'undefined') return null

    try {
      const profileData = localStorage.getItem('oidc_user_profile')

      return profileData ? JSON.parse(profileData) : null
    } catch (error) {
      console.error('[OidcOAuthProvider] Error parsing user profile:', error)

      return null
    }
  }

  /**
   * Clear authentication data
   */
  clearAuthData() {
    if (typeof window === 'undefined') return

    localStorage.removeItem('oidc_access_token')
    localStorage.removeItem('oidc_user_profile')
  }
}
