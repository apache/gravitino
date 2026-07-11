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

import Image from 'next/image'
import { useRouter } from 'next/navigation'
import { useSearchParams } from 'next/navigation'
import { Roboto } from 'next/font/google'
import { Alert, Card, Flex, Typography } from 'antd'
import { cn } from '@/lib/utils/tailwind'
import { useEffect, useState, Suspense } from 'react'

import OidcLogin from './components/OidcLogin'
import DefaultLogin from './components/DefaultLogin'
import BasicLogin from './components/BasicLogin'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'
import { resetMetalakeStore } from '@/lib/store/metalakes'
import { useAppDispatch } from '@/lib/hooks/useStore'

import { to } from '@/lib/utils'
import { getAuthConfigs } from '@/lib/store/auth'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

const { Title } = Typography

const LoginContent = () => {
  const router = useRouter()
  const searchParams = useSearchParams()
  const inactiveReason = searchParams.get('reason') === 'inactive'
  const maxDurationReason = searchParams.get('reason') === 'max_duration'
  const [authType, setAuthType] = useState(null)
  const [providerType, setProviderType] = useState(null)
  const dispatch = useAppDispatch()

  useEffect(() => {
    const detectLoginType = async () => {
      const [, resAuthConfigs] = await to(dispatch(getAuthConfigs()))
      const detectedAuthType = resAuthConfigs?.payload?.authType

      setAuthType(detectedAuthType)

      if (detectedAuthType === 'oauth') {
        try {
          const detectedType = await oauthProviderFactory.getProviderType()
          setProviderType(detectedType)
        } catch (error) {
          setProviderType('default')
        }
      }
    }

    dispatch(resetMetalakeStore())
    detectLoginType()
  }, [dispatch])

  return (
    <Flex justify='center' align='center' style={{ minHeight: 'calc(100vh - 7rem)' }}>
      <Card style={{ width: 480 }} styles={{ body: { padding: 48 } }}>
        <Flex justify='center' align='center' className='mb-8'>
          <Image
            src={`${process.env.NEXT_PUBLIC_BASE_PATH ?? ''}/icons/gravitino.svg`}
            width={24}
            height={24}
            alt='Gravitino Logo'
          />
          <Title level={4} className={cn('!mb-0 ml-2 text-black', fonts.className)}>
            Gravitino
          </Title>
        </Flex>

        {inactiveReason && (
          <Alert
            message='Your session has expired due to inactivity. Please sign in again.'
            type='info'
            showIcon
            closable
            className='mb-6'
          />
        )}

        {maxDurationReason && (
          <Alert
            message='Your session has reached its maximum duration. Please sign in again.'
            type='warning'
            showIcon
            closable
            className='mb-6'
          />
        )}

        {authType === null ? null : authType === 'basic' ? (
          <BasicLogin />
        ) : authType === 'simple' ? (
          <DefaultLogin />
        ) : authType === 'oauth' && providerType === null ? null : authType === 'oauth' && providerType === 'oidc' ? (
          <OidcLogin />
        ) : authType === 'oauth' ? (
          <DefaultLogin />
        ) : null}
      </Card>
    </Flex>
  )
}

const LoginPage = () => {
  return (
    <Suspense>
      <LoginContent />
    </Suspense>
  )
}

export default LoginPage
