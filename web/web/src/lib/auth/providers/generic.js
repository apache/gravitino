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
    this.providerType = 'default'
  }

  async initialize(config) {
    this.config = config
  }

  async getAccessToken() {
    // For generic OAuth providers, tokens are typically obtained through:
    // 1. Client credentials flow (backend handles token exchange)
    // 2. Authorization code flow (stored in localStorage after login)

    // Check localStorage for generic OAuth token
    const token = typeof window !== 'undefined' ? localStorage.getItem('accessToken') : null

    return token
  }

  /**
   * Check if user is currently authenticated
   */
  async isAuthenticated() {
    if (typeof window === 'undefined') {
      return false
    }

    const token = localStorage.getItem('accessToken')

    return !!token
  }

  /**
   * Clear authentication data
   */
  async clearAuthData() {
    if (typeof window === 'undefined') return

    // Clear generic OAuth tokens
    localStorage.removeItem('accessToken')
    localStorage.removeItem('authParams')
    localStorage.removeItem('expiredIn')
    localStorage.removeItem('isIdle')
    localStorage.removeItem('refresh_token')
  }
}
