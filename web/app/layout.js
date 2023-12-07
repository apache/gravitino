/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import '@/lib/styles/globals.css'

import Provider from '@/lib/provider'
import Layout from '@/lib/layout/Layout'

export const metadata = {
  title: 'Gravitino',
  description: 'A high-performance, geo-distributed and federated metadata lake.'
}

const RootLayout = props => {
  const { children } = props

  return (
    <html lang='en' suppressHydrationWarning>
      <body>
        <Provider>{<Layout>{children}</Layout>}</Provider>
      </body>
    </html>
  )
}

export default RootLayout
