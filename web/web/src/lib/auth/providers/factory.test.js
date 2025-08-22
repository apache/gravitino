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

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { OAuthProviderFactory } from '@/lib/auth/providers/factory'

describe('OAuth Provider Factory', () => {
  let fetchMock
  let factory

  // Common mock config
  const oidcConfig = {
    'gravitino.authenticator.oauth.provider': 'oidc',
    'gravitino.authenticator.oauth.authority': 'https://test.example.com',
    'gravitino.authenticator.oauth.clientId': 'test-client'
  }

  const mockSuccessResponse = config => ({
    ok: true,
    json: async () => config
  })

  beforeEach(() => {
    vi.clearAllMocks()

    // Create a fresh factory instance for each test
    factory = new OAuthProviderFactory()

    // Create a proper fetch mock
    fetchMock = vi.fn()
    global.fetch = fetchMock
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('getProvider', () => {
    it('should create OIDC provider when configured', async () => {
      fetchMock.mockResolvedValueOnce(mockSuccessResponse(oidcConfig))

      const provider = await factory.getProvider()

      expect(provider).toBeDefined()
      expect(provider.getType()).toBe('oidc')
      expect(fetchMock).toHaveBeenCalledWith('/configs')
    })

    it('should create default provider when provider not specified', async () => {
      // Mock config response without provider config
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.serverUri': 'https://generic.example.com'
        })
      })

      const provider = await factory.getProvider()

      expect(provider).toBeDefined()
      expect(provider.getType()).toBe('default')
    })

    it('should return cached provider on subsequent calls', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.provider': 'oidc',
          'gravitino.authenticator.oauth.authority': 'https://test.example.com',
          'gravitino.authenticator.oauth.clientId': 'test-client'
        })
      })

      const provider1 = await factory.getProvider()
      const provider2 = await factory.getProvider()

      expect(provider1).toBe(provider2)
      expect(fetchMock).toHaveBeenCalledTimes(1)
    })

    it('should handle config fetch errors', async () => {
      fetchMock.mockRejectedValueOnce(new Error('Network error'))

      await expect(factory.getProvider()).rejects.toThrow('Network error')
    })

    it('should handle non-ok HTTP responses', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error'
      })

      await expect(factory.getProvider()).rejects.toThrow('Failed to fetch OAuth config: 500')
    })

    it('should handle invalid JSON responses', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => {
          throw new Error('Invalid JSON')
        }
      })

      await expect(factory.getProvider()).rejects.toThrow('Invalid JSON')
    })
  })

  describe('provider creation', () => {
    it('should create OIDC provider when provider is set to oidc', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.provider': 'OIDC',
          'gravitino.authenticator.oauth.authority': 'https://oidc.example.com',
          'gravitino.authenticator.oauth.clientId': 'oidc-client'
        })
      })

      const provider = await factory.getProvider()

      expect(provider.getType()).toBe('oidc')
    })

    it('should use default provider when no provider specified', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.serverUri': 'https://generic.example.com'
        })
      })

      const provider = await factory.getProvider()

      expect(provider.getType()).toBe('default')
    })

    it('should use default provider for unknown provider types', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.provider': 'unknown-provider',
          'gravitino.authenticator.oauth.serverUri': 'https://generic.example.com'
        })
      })

      const provider = await factory.getProvider()

      expect(provider.getType()).toBe('default')
    })
  })

  describe('utility methods', () => {
    it('should get access token from provider', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.provider': 'oidc',
          'gravitino.authenticator.oauth.authority': 'https://test.example.com',
          'gravitino.authenticator.oauth.clientId': 'test-client'
        })
      })

      // Mock the provider's getAccessToken method
      const mockToken = 'test-token'
      const provider = await factory.getProvider()
      vi.spyOn(provider, 'getAccessToken').mockResolvedValue(mockToken)

      const token = await factory.getAccessToken()

      expect(token).toBe(mockToken)
    })

    it('should get provider type', async () => {
      fetchMock.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          'gravitino.authenticator.oauth.provider': 'oidc',
          'gravitino.authenticator.oauth.authority': 'https://test.example.com',
          'gravitino.authenticator.oauth.clientId': 'test-client'
        })
      })

      const providerType = await factory.getProviderType()

      expect(providerType).toBe('oidc')
    })
  })
})
