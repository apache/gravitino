/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useContext } from 'react'
import { SettingsContext } from 'src/@core/context/settingsContext'

export const useSettings = () => useContext(SettingsContext)
