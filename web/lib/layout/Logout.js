/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useRouter } from 'next/navigation'

import { Box, IconButton } from '@mui/material'

import Icon from '@/components/Icon'
import { useAppDispatch, useAppSelector } from '../hooks/useStore'
import { logoutAction } from '../store/auth'

const LogoutButton = () => {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const authStore = useAppSelector(state => state.auth)

  const handleLogout = () => {
    dispatch(logoutAction({ router }))
  }

  return (
    <Box>
      {authStore.authToken ? (
        <IconButton onClick={() => handleLogout()}>
          <Icon icon={'bx:exit'} />
        </IconButton>
      ) : null}
    </Box>
  )
}

export default LogoutButton
