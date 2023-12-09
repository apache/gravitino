/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Box, IconButton } from '@mui/material'

import Icon from '@/components/Icon'
import { useAuth } from '../provider/session'
import { useAppSelector } from '../hooks/useStore'

const LogoutButton = () => {
  const auth = useAuth()
  const authStore = useAppSelector(state => state.auth)

  return (
    <Box>
      {authStore.authToken ? (
        <IconButton onClick={() => auth.logout()}>
          <Icon icon={'bx:exit'} />
        </IconButton>
      ) : null}
    </Box>
  )
}

export default LogoutButton
