/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import '@/src/lib/styles/globals.css'

import { Suspense } from 'react'

import { NavigationEvents } from './rootLayout/navigation-events'

import Provider from '@/src/lib/provider'

import Loading from './rootLayout/Loading'
import Layout from './rootLayout/Layout'
import StyledToast from '../components/StyledToast'

export const metadata = {
  title: 'Gravitino',
  description: 'A high-performance, geo-distributed and federated metadata lake.',
  icons: {
    icon: '/ui/icons/gravitino.svg'
  }
}

const RootLayout = ({ children }) => {
  return (
    <html lang='en' suppressHydrationWarning>
      <body>
        <Provider>
          <Suspense fallback={<Loading />}>
            <NavigationEvents />
            <Layout>{children}</Layout>
          </Suspense>
        </Provider>
        <StyledToast />
      </body>
    </html>
  )
}

export default RootLayout
