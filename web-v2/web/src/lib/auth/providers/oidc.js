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
import { UserManager, WebStorageStateStore } from 'oidc-client-ts'

export class OidcOAuthProvider extends BaseOAuthProvider {
  constructor() {
    super()
    this.providerType = 'oidc'
    this.oidcConfig = null
    this.userManager = null
  }

  async initialize(config) {
    this.config = config

    const authority = config['gravitino.authenticator.oauth.authority']
    const clientId = config['gravitino.authenticator.oauth.clientId']
    const scope = config['gravitino.authenticator.oauth.scope'] || 'openid profile email'

    if (!authority || !clientId) {
      throw new Error('OIDC provider requires both authority and clientId to be configured')
    }

    // Use provider name directly from config without hardcoded mapping
    this.oidcConfig = {
      authority: authority,
      client_id: clientId,
      response_type: 'code', // Use Authorization Code flow with PKCE
      scope: scope,
      redirect_uri: `${window.location.origin}/ui/oauth/callback`,
      post_logout_redirect_uri: `${window.location.origin}/ui/oauth/logout`,
      silent_redirect_uri: `${window.location.origin}/ui/oauth/silent-callback`,
      automaticSilentRenew: true,
      silentRequestTimeout: 10000,
      userStore: new WebStorageStateStore({ store: window.localStorage })
    }

    // Create shared UserManager instance
    this.userManager = new UserManager(this.oidcConfig)
  }

  async getAccessToken() {
    if (!this.userManager) {
      return null
    }

    try {
      // Get current user from UserManager (includes expiration checking)
      let user = await this.userManager.getUser()

      if (user && !user.expired) {
        // For JWKS validation, we need the ID token (JWT format), not the access token
        return user.id_token || user.access_token
      }

      if (user && user.expired) {
        try {
          // Attempt silent refresh
          const refreshedUser = await this.userManager.signinSilent()

          // Return ID token for JWKS validation
          return refreshedUser.id_token || refreshedUser.access_token
        } catch (refreshError) {
          // Clear expired tokens
          await this.userManager.removeUser()

          return null
        }
      }

      return null
    } catch (error) {
      return null
    }
  }

  /**
   * Get the shared UserManager instance
   */
  getUserManager() {
    return this.userManager
  }

  getOidcConfig() {
    return this.oidcConfig
  }

  /**
   * Check if user is currently authenticated
   */
  async isAuthenticated() {
    if (typeof window === 'undefined' || !this.userManager) {
      return false
    }

    try {
      const user = await this.userManager.getUser()

      return !!(user && !user.expired)
    } catch (error) {
      return false
    }
  }

  /**
   * Get current user profile
   */
  async getUserProfile() {
    if (typeof window === 'undefined' || !this.userManager) {
      return null
    }

    try {
      const user = await this.userManager.getUser()

      return user ? user.profile : null
    } catch (error) {
      return null
    }
  }

  /**
   * Clear authentication data
   */
  async clearAuthData() {
    if (typeof window === 'undefined') return

    // Clear UserManager data if available
    if (this.userManager) {
      try {
        await this.userManager.removeUser()
      } catch (error) {
        // Silent error handling for production
      }
    }
  }
}
