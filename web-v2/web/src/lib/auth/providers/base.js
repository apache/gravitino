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

/**
 * Base OAuth Provider Interface
 * All OAuth providers should implement these methods
 */
export class BaseOAuthProvider {
  constructor(config) {
    this.config = config
    this.providerType = 'base'
  }

  /**
   * Initialize the provider with configuration
   * @param {Object} config - OAuth configuration from backend
   * @returns {Promise<void>}
   */
  async initialize(config) {
    throw new Error('initialize() must be implemented by OAuth provider')
  }

  /**
   * Get access token for API calls
   * @returns {Promise<string|null>} - Access token or null if not available
   */
  async getAccessToken() {
    throw new Error('getAccessToken() must be implemented by OAuth provider')
  }

  /**
   * Check if user is currently authenticated
   * @returns {Promise<boolean>} - True if authenticated, false otherwise
   */
  async isAuthenticated() {
    throw new Error('isAuthenticated() must be implemented by OAuth provider')
  }

  /**
   * Clear authentication data (logout)
   * @returns {Promise<void>}
   */
  async clearAuthData() {
    throw new Error('clearAuthData() must be implemented by OAuth provider')
  }

  /**
   * Get provider type
   * @returns {string}
   */
  getType() {
    return this.providerType
  }
}
