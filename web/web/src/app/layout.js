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
