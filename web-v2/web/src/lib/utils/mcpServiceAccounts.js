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
 * A service account's config as a form shows it: the issuer its tokens must come
 * from, and the rest of the config as JSON.
 */
export const splitServiceAccountConfig = config => {
  const { iss, ...rest } = config || {}

  return { issuer: iss || '', rest: Object.keys(rest).length ? JSON.stringify(rest, null, 2) : '' }
}

// The config to save from the form's issuer and JSON, or null for none; the issuer field wins over an `iss` in the JSON.
export const serviceAccountConfig = (issuer, json) => {
  let rest = {}
  if (json?.trim()) {
    try {
      rest = JSON.parse(json)
    } catch {
      rest = null
    }
    if (!rest || typeof rest !== 'object' || Array.isArray(rest)) {
      throw new Error('The configuration must be a JSON object')
    }
  }
  const config = issuer?.trim() ? { ...rest, iss: issuer.trim() } : rest

  return Object.keys(config).length ? config : null
}
