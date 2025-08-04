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
  generic: GenericOAuthProvider
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
      console.log('[OAuthProviderFactory] Fetching OAuth configuration...')

      const response = await fetch('/configs')
      if (!response.ok) {
        throw new Error(`Failed to fetch OAuth config: ${response.status}`)
      }

      const config = await response.json()

      // Determine provider type based on configuration
      let providerType = 'generic' // Default to generic (backward compatibility)

      // Check OAuth provider configuration
      const provider = config['gravitino.authenticator.oauth.provider']

      // Use OIDC for any provider that's not 'generic' (or undefined)
      if (provider && provider.toLowerCase() !== 'generic') {
        providerType = 'oidc'
        console.log('[OAuthProviderFactory] OIDC provider detected')
        console.log(`  Provider: ${provider}`)

        // Validate required OIDC configuration
        const authority = config['gravitino.authenticator.oauth.authority']
        const clientId = config['gravitino.authenticator.oauth.client-id']

        if (!authority || !clientId) {
          console.warn(
            '[OAuthProviderFactory] OIDC provider missing required config (authority/client-id), falling back to generic'
          )
          providerType = 'generic'
        }
      } else {
        console.log('[OAuthProviderFactory] Using generic provider')
        if (provider) {
          console.log(`  Provider type: ${provider} (treated as generic)`)
        }
      }
      console.log('[OAuthProviderFactory] Selected provider type:', providerType)

      // Get provider class from registry
      const ProviderClass = PROVIDER_REGISTRY[providerType]
      if (!ProviderClass) {
        console.warn(`[OAuthProviderFactory] Unknown provider type: ${providerType}, falling back to generic`)
        this.currentProvider = new GenericOAuthProvider()
      } else {
        this.currentProvider = new ProviderClass()
      }

      // Initialize the provider with configuration
      await this.currentProvider.initialize(config)

      // Store provider type in localStorage for other components
      if (typeof window !== 'undefined') {
        localStorage.setItem('oauthProvider', providerType)
      }

      console.log('[OAuthProviderFactory] Provider initialized successfully:', this.currentProvider.getType())

      return this.currentProvider
    } catch (error) {
      console.error('[OAuthProviderFactory] Failed to initialize OAuth provider:', error)
      this.configPromise = null
      throw error
    }
  }

  /**
   * Get access token from current provider
   * @returns {Promise<string|null>}
   */
  async getAccessToken() {
    console.log('[OAuthProviderFactory] getAccessToken called')

    const provider = await this.getProvider()
    console.log('[OAuthProviderFactory] Provider type:', provider ? provider.getType() : 'null')

    const token = await provider.getAccessToken()
    console.log('[OAuthProviderFactory] Token retrieved from provider:', !!token)

    return token
  }

  /**
   * Get provider configuration
   * @returns {Promise<Object>}
   */
  async getConfig() {
    const provider = await this.getProvider()

    return provider.getConfig()
  }

  /**
   * Get provider type
   * @returns {Promise<string>}
   */
  async getProviderType() {
    const provider = await this.getProvider()

    return provider.getType()
  }

  /**
   * Check if current provider requires a wrapper component
   * @returns {Promise<boolean>}
   */
  async requiresWrapper() {
    const provider = await this.getProvider()

    return provider.requiresWrapper()
  }

  /**
   * Get wrapper component from current provider
   * @returns {Promise<React.Component|null>}
   */
  async getWrapperComponent() {
    const provider = await this.getProvider()

    return provider.getWrapperComponent()
  }

  /**
   * Reset the factory (useful for testing or provider switching)
   */
  reset() {
    this.currentProvider = null
    this.configPromise = null
  }
}

// Export singleton instance
export const oauthProviderFactory = new OAuthProviderFactory()

// Export factory class for testing
export { OAuthProviderFactory }
