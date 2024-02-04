/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Box } from '@mui/material'

import MetalakeTree from './MetalakeTree'
import { useEffect, useRef, useState } from 'react'

const SidebarLeft = props => {
  const treeWrapper = useRef(null)

  const [height, setHeight] = useState(0)

  useEffect(() => {
    if (treeWrapper.current) {
      const { offsetHeight } = treeWrapper.current
      setHeight(offsetHeight - 20)
    }
  }, [treeWrapper])

  return (
    <Box className={`twc-bg-customs-white`} sx={{ borderRight: theme => `1px solid ${theme.palette.divider}` }}>
      <Box className={`twc-w-[340px] twc-h-full round-tl-md round-bl-md twc-overflow-hidden`}>
        <Box className={'twc-px-5 twc-py-3 twc-flex twc-items-center'}></Box>

        <Box
          className={`twc-overflow-y-hidden`}
          sx={{ p: theme => theme.spacing(2.5, 2.5, 2.5), height: `calc(100% - 1.5rem)` }}
          ref={treeWrapper}
        >
          <MetalakeTree height={height} />
        </Box>
      </Box>
    </Box>
  )
}

export default SidebarLeft
