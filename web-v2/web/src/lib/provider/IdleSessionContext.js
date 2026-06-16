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

import { createContext, useContext } from 'react'

/**
 * Idle timer configuration interface.
 *
 * @typedef {Object} IdleTimeoutConfig
 * @property {number} idleTimeoutMs - Idle timeout in milliseconds (default: 15 * 60 * 1000)
 * @property {number} warningLeadMs - Warning lead time in milliseconds (default: 60 * 1000)
 */

/**
 * Idle timer state type: 'active' | 'warning' | 'expired'
 * @typedef {'active' | 'warning' | 'expired'} IdleTimerState
 */

/**
 * Cross-tab message protocol.
 *
 * @typedef {Object} IdleBroadcastMessage
 * @property {'activity' | 'stay_signed_in' | 'logout' | 'sync'} type - Message type
 * @property {number} timestamp - Timestamp of the activity event
 * @property {string} tabId - Source tab identifier
 */

const defaultContextValue = {
  /** @type {IdleTimerState} */
  state: 'active',

  /** Remaining seconds before timeout (approximate, for display) */
  countdown: 0,

  /** Resets the idle timer as if the user just interacted */
  staySignedIn: () => {},

  /** Triggers immediate logout */
  signOutNow: () => {}
}

const IdleSessionContext = createContext(defaultContextValue)

export const useIdleSession = () => useContext(IdleSessionContext)

export default IdleSessionContext
