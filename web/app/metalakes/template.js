/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Box } from '@mui/material'

const Template = ({ children }) => {
  return (
    <Box className={'metalake-template'} style={{ height: 'calc(100vh - 11rem)' }}>
      {children}
    </Box>
  )
}

export default Template
