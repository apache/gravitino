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

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Button, Result, Spin } from 'antd'
import SectionContainer from '@/components/SectionContainer'
import { MCP_GATEWAY_URI_KEY, createMcpGatewayApi, gatewayErrorMessage, isSignedOut } from '@/lib/api/mcpGateway'
import { useAppSelector } from '@/lib/hooks/useStore'

const McpGatewayContext = createContext(null)

// The gateway's API, and the signed-in user as the gateway knows them.
export const useMcpGateway = () => useContext(McpGatewayContext)

// Wraps the pages for the gateway's admins: Gravitino's service admins, when the gateway
// knows Gravitino (GRAVITINO_URI), and the people in its ADMIN_EMAILS.
export const AdminOnly = ({ children }) => {
  const { me } = useMcpGateway()

  return me.is_admin ? children : <Result status='403' title='Only MCP gateway admins can use this page' />
}

export default function McpGatewayProvider({ children }) {
  const router = useRouter()
  const { systemConfig, authType } = useAppSelector(state => state.auth)
  const gatewayUri = systemConfig?.[MCP_GATEWAY_URI_KEY]

  const [api, uriError] = useMemo(() => {
    try {
      return [gatewayUri ? createMcpGatewayApi(gatewayUri) : null, undefined]
    } catch (err) {
      return [null, err.message]
    }
  }, [gatewayUri])
  const [me, setMe] = useState()
  const [error, setError] = useState()
  const [attempt, setAttempt] = useState(0)
  const value = useMemo(() => ({ api, me }), [api, me])

  useEffect(() => {
    if (!api || authType !== 'oauth') {
      return
    }

    // Ignore an answer that comes after the gateway or the sign-in changed.
    let current = true
    api
      .getMe()
      .then(user => current && setMe(user))
      .catch(err => {
        if (current) {
          isSignedOut(err) ? router.push('/login') : setError(gatewayErrorMessage(err))
        }
      })

    return () => {
      current = false
    }
  }, [api, authType, router, attempt])

  let unavailable = null
  if (systemConfig && !gatewayUri) {
    unavailable = (
      <Result
        status='info'
        title='The MCP gateway is not set up'
        subTitle={`Set ${MCP_GATEWAY_URI_KEY} on the Gravitino server and list it in gravitino.server.visibleConfigs.`}
      />
    )
  } else if (uriError) {
    unavailable = <Result status='error' title='The MCP gateway is not set up' subTitle={uriError} />
  } else if (authType && authType !== 'oauth') {
    unavailable = (
      <Result
        status='info'
        title='MCP servers need an OAuth sign-in'
        subTitle='The MCP gateway accepts the token of an OAuth sign-in to Gravitino.'
      />
    )
  } else if (error) {
    unavailable = (
      <Result
        status='error'
        title='The MCP gateway is unavailable'
        subTitle={error}
        extra={
          <Button
            onClick={() => {
              setError(undefined)
              setAttempt(n => n + 1)
            }}
          >
            Retry
          </Button>
        }
      />
    )
  } else if (!me) {
    unavailable = <Spin className='mt-20 w-full' />
  }

  if (unavailable) {
    return <SectionContainer classProps='block overflow-y-auto'>{unavailable}</SectionContainer>
  }

  return <McpGatewayContext.Provider value={value}>{children}</McpGatewayContext.Provider>
}
