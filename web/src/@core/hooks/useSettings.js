import { useContext } from 'react'
import { SettingsContext } from 'src/@core/context/settingsContext'

export const useSettings = () => useContext(SettingsContext)
