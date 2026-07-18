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

import { useState, useEffect, useRef } from 'react'
import { Modal, Typography, Button, Flex } from 'antd'
import { ClockCircleOutlined } from '@ant-design/icons'

const { Text } = Typography

/**
 * Warning modal displayed before idle session timeout (IST-REQ-003).
 * Shows a live countdown and provides "Stay signed in" / "Sign out now" actions.
 *
 * The modal is non-dismissible by clicking the overlay or pressing Escape
 * to prevent accidental dismissal.
 *
 * @param {Object} props
 * @param {boolean} props.open - Whether the modal is visible
 * @param {number} props.countdownSeconds - Initial countdown value in seconds
 * @param {() => void} props.onStaySignedIn - Callback when user clicks "Stay signed in"
 * @param {() => void} props.onSignOut - Callback when user clicks "Sign out now" or countdown reaches zero
 */
export default function IdleWarningModal({ open, countdownSeconds, onStaySignedIn, onSignOut }) {
  const [remaining, setRemaining] = useState(countdownSeconds)
  const onSignOutRef = useRef(onSignOut)

  // Keep ref in sync with latest callback
  useEffect(() => {
    onSignOutRef.current = onSignOut
  }, [onSignOut])

  // Reset countdown when the modal opens or countdownSeconds changes
  useEffect(() => {
    if (open) {
      setRemaining(countdownSeconds)
    }
  }, [open, countdownSeconds])

  // Countdown timer - only depends on open, not remaining
  // Uses functional setRemaining to avoid recreating interval every second
  useEffect(() => {
    if (!open) {
      return
    }

    const timer = setInterval(() => {
      setRemaining(prev => {
        if (prev <= 1) {
          clearInterval(timer)

          // Auto-logout when countdown reaches zero
          onSignOutRef.current()

          return 0
        }

        return prev - 1
      })
    }, 1000)

    return () => clearInterval(timer)
  }, [open])

  return (
    <Modal
      open={open}
      title={
        <Flex align='center' gap={8}>
          <ClockCircleOutlined style={{ color: '#faad14', fontSize: 20 }} />
          <span>Session Expiring Soon</span>
        </Flex>
      }
      footer={[
        <Button key='signout' danger onClick={onSignOut}>
          Sign out now
        </Button>,
        <Button key='stay' type='primary' onClick={onStaySignedIn}>
          Stay signed in
        </Button>
      ]}
      closable={false}
      maskClosable={false}
      keyboard={false}
      centered
      width={420}
    >
      <div className='py-2'>
        <Text>Your session will expire in {remaining} seconds due to inactivity.</Text>
        <div className='mt-4 text-center'>
          <Text className='text-3xl font-bold text-orange-500'>{remaining}</Text>
          <Text className='ml-2 text-gray-500'>seconds</Text>
        </div>
      </div>
    </Modal>
  )
}
