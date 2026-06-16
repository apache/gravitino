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

import { useEffect, useCallback, useRef, useState } from 'react'
import { useRouter, usePathname } from 'next/navigation'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { useIdleTimeout } from '@/lib/hooks/useIdleTimeout'
import { useBroadcastChannel } from '@/lib/hooks/useBroadcastChannel'
import { logoutAction } from '@/lib/store/auth'
import IdleSessionContext from './IdleSessionContext'
import IdleWarningModal from '@/components/IdleWarningModal'

/**
 * Default idle timeout: 15 minutes in milliseconds.
 * Overridable via NEXT_PUBLIC_IDLE_TIMEOUT_MS environment variable.
 */
const DEFAULT_IDLE_TIMEOUT_MS = (() => {
  const envVal = process.env.NEXT_PUBLIC_IDLE_TIMEOUT_MS
  const parsed = envVal ? Number(envVal) : NaN

  return Number.isFinite(parsed) && parsed > 0 ? parsed : 15 * 60 * 1000
})()

/**
 * Default warning lead time: 60 seconds in milliseconds.
 * Overridable via NEXT_PUBLIC_IDLE_WARNING_LEAD_MS environment variable.
 */
const DEFAULT_WARNING_LEAD_MS = (() => {
  const envVal = process.env.NEXT_PUBLIC_IDLE_WARNING_LEAD_MS
  const parsed = envVal ? Number(envVal) : NaN

  return Number.isFinite(parsed) && parsed > 0 ? parsed : 60 * 1000
})()

/**
 * Converts milliseconds to seconds, rounded up.
 */
function msToSeconds(ms) {
  return Math.ceil(ms / 1000)
}

/**
 * IdleSessionProvider manages the idle timeout state machine and cross-tab
 * coordination (IST-REQ-001 through IST-REQ-006).
 *
 * State machine: active → warning → expired
 *
 * - ACTIVE: Normal operation, idle timer is running
 * - WARNING: Warning lead time has elapsed, modal is shown with countdown
 * - EXPIRED: Timeout reached, logout is triggered
 *
 * @param {Object} props
 * @param {React.ReactNode} props.children
 * @param {number} [props.idleTimeoutMs] - Idle timeout in milliseconds (default: 15 min)
 * @param {number} [props.warningLeadMs] - Warning lead time in milliseconds (default: 60s)
 */
export default function IdleSessionProvider({
  children,
  idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS,
  warningLeadMs = DEFAULT_WARNING_LEAD_MS
}) {
  const router = useRouter()
  const pathname = usePathname()
  const dispatch = useAppDispatch()
  const authType = useAppSelector(state => state.auth.authType)
  const authToken = useAppSelector(state => state.auth.authToken)

  // Only enable idle timeout when the user is authenticated and not on login page
  const isAuthenticated = authType === 'simple' ? !!sessionStorage.getItem('simpleAuthUser') : !!authToken
  const isLoginPage = pathname === '/login'

  const [state, setState] = useState('active')
  const [warningCountdown, setWarningCountdown] = useState(msToSeconds(warningLeadMs))
  const loggedOutRef = useRef(false)
  const wasAuthenticatedRef = useRef(isAuthenticated)

  const {
    isIdle,
    resetActivity: resetIdleActivity,
    idleTimeRemaining
  } = useIdleTimeout({
    idleTimeoutMs: isAuthenticated ? idleTimeoutMs : Number.MAX_SAFE_INTEGER,

    // Pause DOM activity detection during warning state so the modal stays
    // visible until the user explicitly clicks "Stay signed in" or "Sign out now"
    paused: state === 'warning'
  })

  const { sendMessage, onMessage } = useBroadcastChannel()

  // Track warning threshold: when idleTimeRemaining drops below warningLeadMs, show warning
  const warningThreshold = warningLeadMs

  // State machine transitions
  useEffect(() => {
    if (!isAuthenticated) {
      return
    }

    if (isIdle) {
      // Idle time exceeded full timeout → expired
      setState('expired')
    } else if (idleTimeRemaining <= warningThreshold && idleTimeRemaining > 0) {
      // Within warning window
      if (state === 'active') {
        setState('warning')
        setWarningCountdown(msToSeconds(idleTimeRemaining))
      }
    } else if (idleTimeRemaining > warningThreshold) {
      // Back above warning threshold (activity detected)
      if (state !== 'active') {
        setState('active')
      }
    }
  }, [isIdle, idleTimeRemaining, warningThreshold, isAuthenticated])

  // Update warning countdown based on remaining idle time
  useEffect(() => {
    if (state === 'warning') {
      setWarningCountdown(msToSeconds(idleTimeRemaining))
    }
  }, [state, idleTimeRemaining])

  // Handle logout (either from timeout or "Sign out now")
  const handleLogout = useCallback(() => {
    if (loggedOutRef.current) {
      return
    }

    loggedOutRef.current = true
    setState('expired')

    // Broadcast logout with reason to other tabs
    sendMessage({ type: 'logout', reason: 'inactive', timestamp: Date.now() })

    // Dispatch logout action (handles both OAuth and simple auth)
    // Pass reason to show inactivity message on login page
    dispatch(logoutAction({ router, reason: 'inactive' }))
  }, [dispatch, router, sendMessage])

  // Handle "Stay signed in" action (IST-REQ-003)
  const handleStaySignedIn = useCallback(() => {
    setState('active')
    resetIdleActivity()
    setWarningCountdown(msToSeconds(warningLeadMs))

    // Broadcast stay_signed_in to other tabs so they also dismiss warning and stay logged in
    sendMessage({ type: 'stay_signed_in', timestamp: Date.now() })
  }, [resetIdleActivity, warningLeadMs, sendMessage])

  // Cross-tab message handling (IST-REQ-005)
  useEffect(() => {
    onMessage(message => {
      if (message.type === 'activity') {
        // Activity in another tab resets this tab's idle timer and dismisses warning modal
        resetIdleActivity()
        if (state === 'warning') {
          setState('active')
          setWarningCountdown(msToSeconds(warningLeadMs))
        }
      } else if (message.type === 'stay_signed_in') {
        // Another tab clicked "Stay signed in" — dismiss warning and stay logged in
        resetIdleActivity()
        if (state === 'warning') {
          setState('active')
          setWarningCountdown(msToSeconds(warningLeadMs))
        }
      } else if (message.type === 'logout') {
        // Another tab triggered logout — clear local auth state and redirect
        if (!loggedOutRef.current) {
          loggedOutRef.current = true
          dispatch(logoutAction({ router, reason: message.reason }))
        }
      }
    })
  }, [onMessage, resetIdleActivity, router, state, warningLeadMs, dispatch])

  // Broadcast activity to other tabs when this tab has activity (IST-REQ-005)
  // Only broadcast when idleTimeRemaining jumps UP (indicating a timer reset),
  // not on every tick down. This prevents continuously resetting other tabs' timers.
  const prevIdleTimeRemainingRef = useRef(idleTimeRemaining)
  useEffect(() => {
    if (!isAuthenticated) {
      return
    }

    // Detect a jump UP in remaining time, which indicates the idle timer was reset
    // due to user activity. A steady decrease (idle countdown) should not trigger a broadcast.
    if (idleTimeRemaining > prevIdleTimeRemainingRef.current) {
      sendMessage({ type: 'activity', timestamp: Date.now() })
    }

    prevIdleTimeRemainingRef.current = idleTimeRemaining
  }, [idleTimeRemaining, isAuthenticated, sendMessage])

  // Auto-logout when state becomes expired (from isIdle)
  useEffect(() => {
    if (state === 'expired' && !loggedOutRef.current) {
      handleLogout()
    }
  }, [state, handleLogout])

  // Detect authenticated→unauthenticated transition (e.g., manual logout from user menu)
  // and broadcast logout to other tabs
  useEffect(() => {
    const wasAuthenticated = wasAuthenticatedRef.current

    // Reset guard and timer state on unauthenticated→authenticated transition
    // This handles SPA flows where user logs out and logs back in without a full page reload
    if (!wasAuthenticated && isAuthenticated) {
      loggedOutRef.current = false
      setState('active')
      resetIdleActivity()
      setWarningCountdown(msToSeconds(warningLeadMs))
    }

    // Broadcast authenticated→unauthenticated transition (e.g., manual logout from user menu)
    if (wasAuthenticated && !isAuthenticated && !loggedOutRef.current) {
      loggedOutRef.current = true
      sendMessage({ type: 'logout', timestamp: Date.now() })
    }

    wasAuthenticatedRef.current = isAuthenticated
  }, [isAuthenticated, resetIdleActivity, sendMessage, warningLeadMs])

  // Context value
  const contextValue = {
    state,
    countdown: state === 'warning' ? warningCountdown : 0,
    staySignedIn: handleStaySignedIn,
    signOutNow: handleLogout
  }

  return (
    <IdleSessionContext.Provider value={contextValue}>
      {children}
      {isAuthenticated && !isLoginPage && (
        <IdleWarningModal
          open={state === 'warning'}
          countdownSeconds={warningCountdown}
          onStaySignedIn={handleStaySignedIn}
          onSignOut={handleLogout}
        />
      )}
    </IdleSessionContext.Provider>
  )
}
