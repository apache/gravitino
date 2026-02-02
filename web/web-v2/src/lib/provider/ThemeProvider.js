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

import { useEffect, useState } from 'react'

import { ConfigProvider, theme } from 'antd'
import { ThemeProvider as NextThemeProvider } from 'next-themes'
import { useTheme } from 'next-themes'

import Loading from '@/components/Loading'
import { AntdProvider } from './AntdProvider'

export function AntdConfigProvider({ children }) {
  const { theme: nowTheme } = useTheme()

  return (
    <ConfigProvider
      prefixCls='ant'
      iconPrefixCls='anticon'
      theme={{
        algorithm: theme.defaultAlgorithm
      }}
    >
      <ConfigProvider
        theme={{
          token: {
            colorPrimary: '#0369a1',
            borderRadius: 8
          }
        }}
      >
        <AntdProvider>{children}</AntdProvider>
      </ConfigProvider>
    </ConfigProvider>
  )
}

export default function ThemeProvider({ children }) {
  const [mounted, setMounted] = useState(false)

  // useEffect only runs on the client, so now we can safely show the UI
  useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) {
    // use your loading page
    return <Loading />
  }

  return (
    <NextThemeProvider attribute='class' defaultTheme='default' enableSystem={false} disableTransitionOnChange>
      <AntdConfigProvider>{children}</AntdConfigProvider>
    </NextThemeProvider>
  )
}
