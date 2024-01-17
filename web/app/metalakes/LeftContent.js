/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Box } from '@mui/material'

import MetalakeTree from './MetalakeTree'

const SidebarLeft = props => {
  const { routeParams } = props

  return (
    <Box className={`twc-bg-customs-white`} sx={{ borderRight: theme => `1px solid ${theme.palette.divider}` }}>
      <Box className={`twc-w-[340px] twc-h-full round-tl-md round-bl-md twc-overflow-hidden`}>
        <Box className={'twc-px-5 twc-py-3 twc-flex twc-items-center'}></Box>

        <Box
          className={`twc-overflow-y-auto`}
          sx={{ p: theme => theme.spacing(2.5, 2.5, 2.5), height: `calc(100% - 1.5rem)` }}
        >
          <MetalakeTree routeParams={routeParams} />
        </Box>
      </Box>
    </Box>
  )
}

export default SidebarLeft
