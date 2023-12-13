/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Box, CircularProgress } from '@mui/material'

const Loading = () => {
  return (
    <Box className={`twc-h-[100vh] twc-flex twc-items-center twc-flex-col twc-justify-center`}>
      <CircularProgress disableShrink sx={{ mt: 6 }} />
    </Box>
  )
}

export default Loading
