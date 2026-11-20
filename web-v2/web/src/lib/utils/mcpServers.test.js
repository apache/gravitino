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

import { describe, expect, it } from 'vitest'
import { hasBasicAuth, serverFields } from './mcpServers'

describe('serverFields', () => {
  it('keeps the current secrets when their fields are empty', () => {
    expect(serverFields({ name: 'Trino', oauth_client_secret: '', basic_auth_password: undefined })).toEqual({
      name: 'Trino'
    })
    expect(serverFields({ oauth_client_secret: 's3cret', basic_auth_password: 'pw' })).toEqual({
      oauth_client_secret: 's3cret',
      basic_auth_password: 'pw'
    })
  })

  it('sends an empty optional URL as none', () => {
    expect(serverFields({ oauth_metadata_url: '' })).toEqual({ oauth_metadata_url: null })
  })

  it('removes the client secret when asked to, by sending an empty one', () => {
    expect(serverFields({ name: 'Trino', remove_oauth_client_secret: true })).toEqual({
      name: 'Trino',
      oauth_client_secret: ''
    })
    expect(serverFields({ oauth_client_secret: 'typed', remove_oauth_client_secret: true })).toEqual({
      oauth_client_secret: ''
    })
    expect(serverFields({ name: 'Trino', remove_oauth_client_secret: false })).toEqual({ name: 'Trino' })
  })
})

describe('hasBasicAuth', () => {
  it('is true when people or service accounts already use basic auth', () => {
    expect(hasBasicAuth({ auth_type: 'basic', service_auth_type: 'passthrough' })).toBe(true)
    expect(hasBasicAuth({ auth_type: 'oauth', service_auth_type: 'basic' })).toBe(true)
  })

  it('is false for a new server, or one without basic auth', () => {
    expect(hasBasicAuth(undefined)).toBe(false)
    expect(hasBasicAuth({ auth_type: 'oauth', service_auth_type: 'passthrough' })).toBe(false)
  })
})
