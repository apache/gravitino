'use client'

import { IconButton } from '@mui/material'

import Icon from '@/components/Icon'
import { useTheme } from '../provider/theme'
import useHasMounted from '../hooks/useHasMounted'

const ToggleMode = props => {
  const { mode, toggleTheme } = useTheme()

  const hasMounted = useHasMounted()

  return (
    <>
      {hasMounted && (
        <IconButton color='inherit' aria-haspopup='true' onClick={toggleTheme}>
          <Icon icon={mode === 'dark' ? 'bx:sun' : 'bx:moon'} />
        </IconButton>
      )}
    </>
  )
}

export default ToggleMode
