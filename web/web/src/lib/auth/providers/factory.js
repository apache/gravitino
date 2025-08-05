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

import { OidcOAuthProvider } from './oidc'
import { GenericOAuthProvider } from './generic'

// Registry of available OAuth providers
const PROVIDER_REGISTRY = {
  oidc: OidcOAuthProvider,
  default: GenericOAuthProvider
}

/**
 * OAuth Provider Factory
 * Creates and manages OAuth provider instances
 */
class OAuthProviderFactory {
  constructor() {
    this.currentProvider = null
    this.configPromise = null
  }

  /**
   * Get or create OAuth provider based on configuration
   * @returns {Promise<BaseOAuthProvider>}
   */
  async getProvider() {
    if (this.currentProvider) {
      return this.currentProvider
    }

    if (this.configPromise) {
      await this.configPromise

      return this.currentProvider
    }

    this.configPromise = this._initializeProvider()
    await this.configPromise

    return this.currentProvider
  }

  async _initializeProvider() {
    try {
      const response = await fetch('/configs')
      if (!response.ok) {
        throw new Error(`Failed to fetch OAuth config: ${response.status}`)
      }

      const config = await response.json()
      let providerType = 'default'
      const provider = config['gravitino.authenticator.oauth.provider']

      if (provider) {
        const normalizedProvider = provider.toLowerCase()
        if (normalizedProvider === 'oidc') {
          providerType = 'oidc'
        }

        // If provider is set to something else, fall back to default
      }

      const ProviderClass = PROVIDER_REGISTRY[providerType]
      this.currentProvider = new ProviderClass()
      await this.currentProvider.initialize(config)

      return this.currentProvider
    } catch (error) {
      this.configPromise = null
      throw error
    }
  }

  /**
   * Get access token from current provider
   * @returns {Promise<string|null>}
   */
  async getAccessToken() {
    const provider = await this.getProvider()

    return await provider.getAccessToken()
  }

  /**
   * Get provider type
   * @returns {Promise<string>}
   */
  async getProviderType() {
    const provider = await this.getProvider()

    return provider.getType()
  }
}

// Export singleton instance
export const oauthProviderFactory = new OAuthProviderFactory()

// Export factory class for testing
export { OAuthProviderFactory }
