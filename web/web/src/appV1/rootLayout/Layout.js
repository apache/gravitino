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

import { Box, Fab } from '@mui/material'

import Icon from '@/components/Icon'

import AppBar from './AppBar'
import Footer from './Footer'
import Loading from './Loading'
import ScrollToTop from './ScrollToTop'
import dynamic from 'next/dynamic'

const DynamicMainContent = dynamic(() => import('./MainContent'), {
  loading: () => <Loading />,
  ssr: false
})

const Layout = ({ children, scrollToTop }) => {
  return (
    <div className={'layout-wrapper twc-h-full twc-flex twc-overflow-clip'}>
      <Box className={'layout-content-wrapper twc-flex twc-grow twc-min-h-[100vh] twc-min-w-0 twc-flex-col'}>
        <Box
          className={
            'app-bar-bg-blur twc-top-0 twc-z-10 twc-w-full twc-fixed twc-backdrop-saturate-200 twc-backdrop-blur-[10px] twc-bg-customs-lightBg twc-h-[0.8125rem]'
          }
        />
        <AppBar />
        <DynamicMainContent>{children}</DynamicMainContent>
        <Footer />
        {scrollToTop ? (
          scrollToTop(props)
        ) : (
          <ScrollToTop className='mui-fixed'>
            <Fab color='primary' size='small'>
              <Icon icon='bx:up-arrow-alt' />
            </Fab>
          </ScrollToTop>
        )}
      </Box>
    </div>
  )
}

export default Layout
