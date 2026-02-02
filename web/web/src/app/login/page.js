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
import { Box, Card, Grid, CardContent, Typography } from '@mui/material'
import clsx from 'clsx'
import { useEffect, useState } from 'react'

import OidcLogin from './components/OidcLogin'
import DefaultLogin from './components/DefaultLogin'
import { useAuth } from '@/lib/provider/session'
import { oauthProviderFactory } from '@/lib/auth/providers/factory'

const fonts = Roboto({ subsets: ['latin'], weight: ['400'], display: 'swap' })

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
    <Grid container spacing={2} sx={{ justifyContent: 'center', alignItems: 'center', height: '100%' }}>
      <Box>
        <Card sx={{ width: 480 }}>
          <CardContent className='twc-p-12'>
            <Box className='twc-mb-8 twc-flex twc-items-center twc-justify-center'>
              <Image
                src={`${process.env.NEXT_PUBLIC_BASE_PATH ?? ''}/icons/gravitino.svg`}
                width={24}
                height={24}
                alt='Gravitino Logo'
              />
              <Typography variant='h6' className={clsx('twc-text-[black] twc-ml-2 twc-text-[1.5rem]', fonts.className)}>
                Gravitino
              </Typography>
            </Box>

            {useOidcLogin ? <OidcLogin /> : <DefaultLogin />}
          </CardContent>
        </Card>
      </Box>
    </Grid>
  )
}

export default LoginPage
