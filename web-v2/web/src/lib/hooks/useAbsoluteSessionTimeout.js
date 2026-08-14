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

import { useEffect, useRef, useState, useCallback } from 'react'

/**
 * localStorage key for the session start timestamp.
 * Stored in localStorage (not sessionStorage) so all tabs from the same
 * browser share the same session start time.
 */
const SESSION_START_KEY = 'grtv-session-start'

/**
 * Default maximum session duration: 5 hours in milliseconds.
 * Overridable via NEXT_PUBLIC_MAX_SESSION_DURATION_MS environment variable.
 */
const DEFAULT_MAX_SESSION_DURATION_MS = (() => {
  const envVal = process.env.NEXT_PUBLIC_MAX_SESSION_DURATION_MS
  const parsed = envVal ? Number(envVal) : NaN

  return Number.isFinite(parsed) && parsed > 0 ? parsed : 5 * 60 * 60 * 1000
})()

/**
 * Custom hook for absolute session duration enforcement.
 *
 * Unlike idle timeout (which resets on user activity), the absolute session
 * timer starts when the user logs in and never resets. After the configured
 * duration (default: 5 hours), the session is forcibly terminated.
 *
 * The session start time is stored in localStorage so all tabs from the same
 * browser share the same absolute deadline.
 *
 * @param {Object} options
 * @param {boolean} options.isAuthenticated - Whether the user is currently authenticated.
 *   When true and no session start exists, the current time is recorded.
 *   When false, the session start is cleared.
 * @param {number} [options.maxDurationMs] - Maximum session duration in milliseconds.
 *   Defaults to NEXT_PUBLIC_MAX_SESSION_DURATION_MS env var or 5 hours.
 * @returns {{ isExpired: boolean, remainingMs: number, clearSession: () => void }}
 */
export function useAbsoluteSessionTimeout({ isAuthenticated, maxDurationMs = DEFAULT_MAX_SESSION_DURATION_MS } = {}) {
  const [isExpired, setIsExpired] = useState(false)
  const [remainingMs, setRemainingMs] = useState(maxDurationMs)
  const intervalRef = useRef(null)

  // Record session start time when user becomes authenticated
  useEffect(() => {
    if (isAuthenticated) {
      const existing = localStorage.getItem(SESSION_START_KEY)

      if (existing) {
        // Session start exists (e.g., another tab logged in). Recalculate
        // expiry so that if a new login refreshed the timestamp, this tab
        // clears its stale isExpired flag.
        const start = Number(existing)
        if (!Number.isFinite(start) || start <= 0) {
          const now = Date.now()
          localStorage.setItem(SESSION_START_KEY, String(now))
          setRemainingMs(maxDurationMs)
          setIsExpired(false)

          return
        }

        const elapsed = Date.now() - start
        const remaining = Math.max(0, maxDurationMs - elapsed)

        setRemainingMs(remaining)
        if (remaining > 0) {
          setIsExpired(false)
        }
      } else {
        localStorage.setItem(SESSION_START_KEY, String(Date.now()))
      }
    } else {
      // Clear session start when user is not authenticated
      localStorage.removeItem(SESSION_START_KEY)
      setIsExpired(false)
      setRemainingMs(maxDurationMs)
    }
  }, [isAuthenticated, maxDurationMs])

  // Poll every second to check if the absolute timeout has been reached
  useEffect(() => {
    if (!isAuthenticated) {
      return
    }

    const check = () => {
      const startStr = localStorage.getItem(SESSION_START_KEY)

      if (!startStr) {
        return
      }

      const start = Number(startStr)

      // Guard against corrupted or tampered values
      if (!Number.isFinite(start) || start <= 0) {
        const now = Date.now()

        localStorage.setItem(SESSION_START_KEY, String(now))
        setIsExpired(false)
        setRemainingMs(maxDurationMs)

        return
      }

      const now = Date.now()
      const elapsed = now - start
      const remaining = Math.max(0, maxDurationMs - elapsed)

      setRemainingMs(remaining)
      setIsExpired(remaining <= 0)
    }

    // Check immediately, then every second
    check()
    intervalRef.current = setInterval(check, 1000)

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }
  }, [isAuthenticated, maxDurationMs])

  /**
   * Clears the session start time. Called after logout to reset state.
   */
  const clearSession = useCallback(() => {
    localStorage.removeItem(SESSION_START_KEY)
    setIsExpired(false)
    setRemainingMs(maxDurationMs)
  }, [maxDurationMs])

  return { isExpired, remainingMs, clearSession }
}
