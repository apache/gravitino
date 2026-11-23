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
import { serviceAccountConfig, splitServiceAccountConfig } from './mcpServiceAccounts'

describe('service account config', () => {
  it('keeps the pinned issuer apart from the rest of the config', () => {
    const config = {
      iss: 'https://oidc.example.com',
      downstream_credentials: { trino: { secret_ref: 'mcp-gateway-sa-x' } }
    }

    expect(splitServiceAccountConfig(config)).toEqual({
      issuer: 'https://oidc.example.com',
      rest: '{\n  "downstream_credentials": {\n    "trino": {\n      "secret_ref": "mcp-gateway-sa-x"\n    }\n  }\n}'
    })
    expect(splitServiceAccountConfig(null)).toEqual({ issuer: '', rest: '' })
  })

  it('puts the issuer and the rest back together', () => {
    expect(serviceAccountConfig(' https://oidc.example.com ', '{"a": 1}')).toEqual({
      iss: 'https://oidc.example.com',
      a: 1
    })
    expect(serviceAccountConfig('', '')).toBeNull()
    expect(serviceAccountConfig('', '{}')).toBeNull()
  })

  it('lets the issuer field win over an issuer in the JSON', () => {
    expect(serviceAccountConfig('https://field.example.com', '{"iss": "https://json.example.com"}')).toEqual({
      iss: 'https://field.example.com'
    })
    expect(serviceAccountConfig('', '{"iss": "https://json.example.com"}')).toEqual({
      iss: 'https://json.example.com'
    })
  })

  it('refuses config that is not a JSON object', () => {
    expect(() => serviceAccountConfig('', '[1]')).toThrow('a JSON object')
    expect(() => serviceAccountConfig('', '{oops')).toThrow('a JSON object')
  })
})
