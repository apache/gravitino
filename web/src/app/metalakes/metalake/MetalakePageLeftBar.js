/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect, useRef, useState } from 'react'
import MetalakeTree from './MetalakeTree'
import { Box } from '@mui/material'

const MetalakePageLeftBar = () => {
  const treeWrapper = useRef(null)

  const [height, setHeight] = useState(0)

  useEffect(() => {
    if (treeWrapper.current) {
      const { offsetHeight } = treeWrapper.current
      setHeight(offsetHeight - 20)
    }
  }, [treeWrapper])

  return (
    <Box
      className={`twc-overflow-y-hidden`}
      sx={{ p: theme => theme.spacing(2.5, 2.5, 2.5), height: `calc(100% - 1.5rem)` }}
      ref={treeWrapper}
    >
      <MetalakeTree height={height} />
    </Box>
  )
}

export default MetalakePageLeftBar
