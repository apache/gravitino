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
import { Roboto } from 'next/font/google'
import { Card, Flex, Typography } from 'antd'
import { cn } from '@/lib/utils/tailwind'
import { useEffect, useState } from 'react'

import OidcLogin from './components/OidcLogin'
import DefaultLogin from './components/DefaultLogin'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

const { Title } = Typography

const LoginPage = () => {
  const [providerType, setProviderType] = useState(null)

  useEffect(() => {
    const detectProviderType = async () => {
      try {
        const detectedType = await oauthProviderFactory.getProviderType()
        setProviderType(detectedType)
      } catch (error) {
        setProviderType('default') // fallback to default provider
      }
    }

    detectProviderType()
  }, [])

  const useOidcLogin = providerType === 'oidc'

  return (
    <Flex justify='center' align='center' style={{ minHeight: '100vh' }}>
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

        {useOidcLogin ? <OidcLogin /> : <DefaultLogin />}
      </Card>
    </Flex>
  )
}

export default LoginPage
