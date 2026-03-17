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

'use client'

import { useEffect } from 'react'
import { UserManager, WebStorageStateStore } from 'oidc-client-ts'

export default function SilentCallback() {
  useEffect(() => {
    handleSilentCallback()
  }, [])

  const handleSilentCallback = async () => {
    try {
      const userManager = new UserManager({
        userStore: new WebStorageStateStore({ store: window.localStorage })
      })
      await userManager.signinSilentCallback()
    } catch (error) {
      // Silent callback failed, fall back to normal login
      window.parent.postMessage({ type: 'SILENT_CALLBACK_ERROR', error: error.message }, window.location.origin)
    }
  }

  return null // This page should be invisible
}
