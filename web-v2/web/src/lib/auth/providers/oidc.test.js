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

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { OidcOAuthProvider } from '@/lib/auth/providers/oidc'
import { UserManager } from 'oidc-client-ts'

// Mock oidc-client-ts
vi.mock('oidc-client-ts', () => ({
  UserManager: vi.fn(),
  WebStorageStateStore: vi.fn()
}))

describe('OidcOAuthProvider', () => {
  let provider
  let mockUserManager

  beforeEach(() => {
    // Reset all mocks
    vi.clearAllMocks()

    // Create mock UserManager
    mockUserManager = {
      getUser: vi.fn(),
      signinRedirect: vi.fn(),
      signinRedirectCallback: vi.fn(),
      signinSilent: vi.fn(),
      removeUser: vi.fn()
    }

    UserManager.mockImplementation(() => mockUserManager)

    provider = new OidcOAuthProvider()
  })

  describe('initialization', () => {
    it('should initialize with correct provider type', () => {
      expect(provider.providerType).toBe('oidc')
      expect(provider.oidcConfig).toBeNull()
      expect(provider.userManager).toBeNull()
    })

    it('should throw error when authority is missing', async () => {
      const config = {
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }

      await expect(provider.initialize(config)).rejects.toThrow(
        'OIDC provider requires both authority and clientId to be configured'
      )
    })

    it('should throw error when clientId is missing', async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com'
      }

      await expect(provider.initialize(config)).rejects.toThrow(
        'OIDC provider requires both authority and clientId to be configured'
      )
    })

    it('should initialize successfully with valid config', async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client',
        'gravitino.authenticator.oauth.scope': 'openid profile'
      }

      await provider.initialize(config)

      expect(provider.config).toBe(config)
      expect(provider.oidcConfig).toEqual({
        authority: 'https://test.example.com',
        client_id: 'test-client',
        response_type: 'code',
        scope: 'openid profile',
        redirect_uri: `${window.location.origin}/ui/oauth/callback`,
        post_logout_redirect_uri: `${window.location.origin}/ui/oauth/logout`,
        silent_redirect_uri: `${window.location.origin}/ui/oauth/silent-callback`,
        automaticSilentRenew: true,
        silentRequestTimeout: 10000,
        userStore: expect.any(Object)
      })
      expect(UserManager).toHaveBeenCalledWith(provider.oidcConfig)
    })

    it('should use default scope when not provided', async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }

      await provider.initialize(config)

      expect(provider.oidcConfig.scope).toBe('openid profile email')
    })
  })

  describe('getAccessToken', () => {
    beforeEach(async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }
      await provider.initialize(config)
    })

    it('should return null when userManager is not available', async () => {
      provider.userManager = null
      const token = await provider.getAccessToken()
      expect(token).toBeNull()
    })

    it('should return id_token for valid user', async () => {
      const mockUser = {
        id_token: 'test-id-token',
        access_token: 'test-access-token',
        expired: false
      }
      mockUserManager.getUser.mockResolvedValue(mockUser)

      const token = await provider.getAccessToken()

      expect(token).toBe('test-id-token')
    })

    it('should return access_token when id_token is not available', async () => {
      const mockUser = {
        access_token: 'test-access-token',
        expired: false
      }
      mockUserManager.getUser.mockResolvedValue(mockUser)

      const token = await provider.getAccessToken()

      expect(token).toBe('test-access-token')
    })

    it('should attempt silent refresh for expired user', async () => {
      const expiredUser = { expired: true }

      const refreshedUser = {
        id_token: 'new-id-token',
        expired: false
      }

      mockUserManager.getUser.mockResolvedValue(expiredUser)
      mockUserManager.signinSilent.mockResolvedValue(refreshedUser)

      const token = await provider.getAccessToken()

      expect(mockUserManager.signinSilent).toHaveBeenCalled()
      expect(token).toBe('new-id-token')
    })

    it('should handle silent refresh failure', async () => {
      const expiredUser = { expired: true }

      mockUserManager.getUser.mockResolvedValue(expiredUser)
      mockUserManager.signinSilent.mockRejectedValue(new Error('Refresh failed'))
      mockUserManager.removeUser.mockResolvedValue()

      const token = await provider.getAccessToken()

      expect(mockUserManager.removeUser).toHaveBeenCalled()
      expect(token).toBeNull()
    })

    it('should return null for general errors', async () => {
      mockUserManager.getUser.mockRejectedValue(new Error('General error'))

      const token = await provider.getAccessToken()

      expect(token).toBeNull()
    })
  })

  describe('isAuthenticated', () => {
    beforeEach(async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }
      await provider.initialize(config)
    })

    it('should return false in server environment', async () => {
      // Mock server environment
      const originalWindow = global.window
      delete global.window

      const isAuth = await provider.isAuthenticated()

      expect(isAuth).toBe(false)

      global.window = originalWindow
    })

    it('should return true for valid user', async () => {
      const mockUser = { expired: false }
      mockUserManager.getUser.mockResolvedValue(mockUser)

      const isAuth = await provider.isAuthenticated()

      expect(isAuth).toBe(true)
    })

    it('should return false for expired user', async () => {
      const mockUser = { expired: true }
      mockUserManager.getUser.mockResolvedValue(mockUser)

      const isAuth = await provider.isAuthenticated()

      expect(isAuth).toBe(false)
    })

    it('should return false when no user', async () => {
      mockUserManager.getUser.mockResolvedValue(null)

      const isAuth = await provider.isAuthenticated()

      expect(isAuth).toBe(false)
    })
  })

  describe('getUserProfile', () => {
    beforeEach(async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }
      await provider.initialize(config)
    })

    it('should return user profile when available', async () => {
      const mockProfile = { sub: 'user-id', name: 'Test User' }
      const mockUser = { profile: mockProfile }
      mockUserManager.getUser.mockResolvedValue(mockUser)

      const profile = await provider.getUserProfile()

      expect(profile).toBe(mockProfile)
    })

    it('should return null when no user', async () => {
      mockUserManager.getUser.mockResolvedValue(null)

      const profile = await provider.getUserProfile()

      expect(profile).toBeNull()
    })
  })

  describe('clearAuthData', () => {
    beforeEach(async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }
      await provider.initialize(config)
    })

    it('should call removeUser on userManager', async () => {
      mockUserManager.removeUser.mockResolvedValue()

      await provider.clearAuthData()

      expect(mockUserManager.removeUser).toHaveBeenCalled()
    })

    it('should handle errors silently', async () => {
      mockUserManager.removeUser.mockRejectedValue(new Error('Remove failed'))

      await expect(provider.clearAuthData()).resolves.not.toThrow()
    })
  })

  describe('utility methods', () => {
    beforeEach(async () => {
      const config = {
        'gravitino.authenticator.oauth.authority': 'https://test.example.com',
        'gravitino.authenticator.oauth.clientId': 'test-client'
      }
      await provider.initialize(config)
    })

    it('should return userManager instance', () => {
      expect(provider.getUserManager()).toBe(mockUserManager)
    })

    it('should return oidc config', () => {
      expect(provider.getOidcConfig()).toBe(provider.oidcConfig)
    })
  })
})
