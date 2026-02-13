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
import { Box, Fab, IconButton, Paper, Stack, Typography } from '@mui/material'
import CloseIcon from '@mui/icons-material/Close'

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

const WEB_V2_NOTICE_DISMISSED_KEY = 'gravitino:webV2NoticeDismissed'

const Layout = ({ children, scrollToTop }) => {
  const [showNotice, setShowNotice] = useState(true)

  useEffect(() => {
    try {
      const dismissed = localStorage.getItem(WEB_V2_NOTICE_DISMISSED_KEY)
      if (dismissed === 'true') {
        setShowNotice(false)
      }
    } catch (error) {
      // Ignore storage errors and keep default behavior.
    }
  }, [])

  const handleDismissNotice = () => {
    setShowNotice(false)
    try {
      localStorage.setItem(WEB_V2_NOTICE_DISMISSED_KEY, 'true')
    } catch (error) {
      // Ignore storage errors.
    }
  }

  return (
    <div className={'layout-wrapper twc-h-full twc-flex twc-overflow-clip'}>
      <Box className={'layout-content-wrapper twc-flex twc-grow twc-min-h-[100vh] twc-min-w-0 twc-flex-col'}>
        {showNotice && (
          <Box
            sx={{
              position: 'fixed',
              top: 16,
              right: 16,
              zIndex: 1200,
              maxWidth: 520
            }}
          >
            <Paper variant='outlined' sx={{ p: 4, bgcolor: '#fff', boxShadow: '0 4px 12px rgba(0, 0, 0, 0.12)' }}>
              <Stack direction='row' spacing={1.5} alignItems='flex-start'>
                <Box sx={{ flex: 1 }}>
                  <Typography variant='body2' component='div'>
                    <Box component='span' sx={{ fontWeight: 600 }}>
                      Try the new Web V2 UI.
                    </Box>
                    <Box component='div' sx={{ fontFamily: 'monospace', mt: 1 }}>
                      # In &lt;path-to-gravitino&gt;/conf/gravitino-env.sh
                      <br />
                      GRAVITINO_USE_WEB_V2=true
                    </Box>
                    <Box component='div' sx={{ fontFamily: 'monospace', mt: 1 }}>
                      &lt;path-to-gravitino&gt;/bin/gravitino.sh restart
                    </Box>
                  </Typography>
                </Box>
                <IconButton aria-label='close notice' size='small' onClick={handleDismissNotice}>
                  <CloseIcon fontSize='small' />
                </IconButton>
              </Stack>
            </Paper>
          </Box>
        )}
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
