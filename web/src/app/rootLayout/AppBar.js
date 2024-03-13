/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import Link from 'next/link'
import Image from 'next/image'

import { Box, AppBar as MuiAppBar, Toolbar, Stack, Typography } from '@mui/material'

import VersionView from './VersionView'
import LogoutButton from './Logout'

const AppBar = props => {
  return (
    <MuiAppBar
      elevation={6}
      position={'sticky'}
      className={
        'layout-navbar-container twc-px-6 twc-items-center twc-justify-center twc-transition-[border-bottom] twc-ease-in-out twc-duration-200 twc-bg-customs-white'
      }
    >
      <Box className={'layout-navbar twc-w-full'}>
        <Toolbar className={'navbar-content-container twc-mx-auto [@media(min-width:1440px)]:twc-max-w-[1440px]'}>
          <Box className={'app-bar-content twc-w-full twc-flex twc-items-center twc-justify-between'}>
            <Link href='/' className={'twc-flex twc-items-center twc-no-underline twc-mr-8'}>
              <Image src='/ui/icons/gravitino.svg' width={32} height={32} alt='logo' />
              <Typography
                variant='h5'
                className={
                  'logoText twc-ml-2 twc-leading-none twc-font-bold twc-tracking-[-0.45px] twc-normal-case twc-text-[1.75rem]'
                }
              >
                Gravitino
              </Typography>
            </Link>
            <Box className={'app-bar-content-right twc-flex twc-items-center'}>
              <Stack direction='row' spacing={2} alignItems='center'>
                <VersionView />
                <LogoutButton />
              </Stack>
            </Box>
          </Box>
        </Toolbar>
      </Box>
    </MuiAppBar>
  )
}

export default AppBar
