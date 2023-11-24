/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Box from '@mui/material/Box'
import VersionView from '../VersionView'

const AppBarContent = props => {
  return (
    <Box sx={{ display: 'flex', alignItems: 'center' }}>
      <VersionView />
    </Box>
  )
}

export default AppBarContent
