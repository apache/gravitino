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

import { useRouter } from 'next/navigation'

import { Box, IconButton } from '@mui/material'

import Icon from '@/components/Icon'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { logoutAction } from '@/lib/store/auth'

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
