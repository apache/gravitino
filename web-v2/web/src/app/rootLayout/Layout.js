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

import { App, Layout } from 'antd'
import { SiteHeader } from './SiteHeader'
import Footer from './Footer'
import Loading from '@/components/Loading'
import ScrollToTop from './ScrollToTop'
import dynamic from 'next/dynamic'

const DynamicMainContent = dynamic(() => import('./MainContent'), {
  loading: () => <Loading />,
  ssr: false
})

const AppLayout = ({ children, scrollToTop = true }) => {
  return (
    <App>
      <Layout className='min-h-screen text-clip'>
        <SiteHeader />
        <DynamicMainContent className='relative grow'>{children}</DynamicMainContent>
        <Footer />
        {scrollToTop && <ScrollToTop />}
      </Layout>
    </App>
  )
}

export default AppLayout
