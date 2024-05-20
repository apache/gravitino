/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import '@/lib/styles/globals.css'

import { NavigationEvents } from './rootLayout/navigation-events'
import Provider from '@/lib/provider'
import Layout from './rootLayout/Layout'
import StyledToast from '../components/StyledToast'

import '../lib/icons/iconify-icons.css'

export const metadata = {
  title: 'Gravitino',
  description: 'A high-performance, geo-distributed and federated metadata lake.',
  icons: {
    icon: process.env.NEXT_PUBLIC_BASE_PATH + '/icons/gravitino.svg'
  }
}

const RootLayout = ({ children }) => {
  return (
    <html lang='en' suppressHydrationWarning>
      <body>
        <Provider>
          <NavigationEvents />
          <Layout>{children}</Layout>
        </Provider>
        <StyledToast />
      </body>
    </html>
  )
}

export default RootLayout
