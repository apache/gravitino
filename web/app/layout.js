/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import '@/lib/styles/globals.css'

import { Suspense } from 'react'

import { NavigationEvents } from '@/lib/layout/navigation-events'

import Provider from '@/lib/provider'
import Layout from '@/lib/layout/Layout'

export const metadata = {
  title: 'Gravitino',
  description: 'A high-performance, geo-distributed and federated metadata lake.'
}

import Loading from '@/lib/layout/Loading'

const RootLayout = props => {
  const { children } = props

  return (
    <html lang='en' suppressHydrationWarning>
      <body>
        <Provider>
          {
            <Suspense fallback={<Loading />}>
              <NavigationEvents />
              <Layout>{children}</Layout>
            </Suspense>
          }
        </Provider>
      </body>
    </html>
  )
}

export default RootLayout
