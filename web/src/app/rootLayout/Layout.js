/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import dynamic from 'next/dynamic'

import { Box, Fab } from '@mui/material'

import Icon from '@/components/Icon'

import AppBar from './AppBar'
import Footer from './Footer'
import MainContent from './MainContent'
import ScrollToTop from './ScrollToTop'

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
        <MainContent>{children}</MainContent>
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

// ** use dynamic export instead of export default Layout
export default dynamic(() => Promise.resolve(Layout), { ssr: false })
