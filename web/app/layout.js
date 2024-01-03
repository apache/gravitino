/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import '@/lib/styles/globals.css'

import { Suspense } from 'react'

import { NavigationEvents } from '@/lib/layout/navigation-events'

import Provider from '@/lib/provider'
import Layout from '@/lib/layout/Layout'

import Loading from '@/lib/layout/Loading'

import { Toaster } from 'react-hot-toast'

export const metadata = {
  title: 'Gravitino',
  description: 'A high-performance, geo-distributed and federated metadata lake.'
}

const RootLayout = props => {
  const { children } = props

  return (
    <html lang='en' suppressHydrationWarning>
      <body>
        <Provider>
          <Suspense fallback={<Loading />}>
            <NavigationEvents />
            <Layout>{children}</Layout>
          </Suspense>
        </Provider>
        <Toaster position='top-right' />
      </body>
    </html>
  )
}

export default RootLayout
