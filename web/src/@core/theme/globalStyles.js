/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useSettings } from 'src/@core/hooks/useSettings'

const GlobalStyles = theme => {
  const { settings } = useSettings()
  const { mode } = settings

  const perfectScrollbarThumbBgColor = () => {
    if (mode === 'light') {
      return 'rgba(86, 106, 127, 0.25) !important'
    } else {
      return 'rgba(203, 203, 226, 0.4) !important'
    }
  }

  return {
    '.demo-space-x > *': {
      marginTop: '1rem !important',
      marginRight: '1rem !important',
      'body[dir="rtl"] &': {
        marginRight: '0 !important',
        marginLeft: '1rem !important'
      }
    },
    '.demo-space-y > *:not(:last-of-type)': {
      marginBottom: '1rem'
    },
    '.MuiGrid-container.match-height .MuiCard-root': {
      height: '100%'
    },
    '.ps__rail-y': {
      zIndex: 1,
      width: 12,
      right: '0 !important',
      left: 'auto !important',
      '&:hover, &:focus, &.ps--clicking': {
        backgroundColor:
          theme.palette.mode === 'light'
            ? 'rgba(86, 106, 127, 0.1) !important'
            : 'rgba(203, 203, 226, 0.15) !important',
        '& .ps__thumb-y': {
          width: 8
        }
      },
      '& .ps__thumb-y': {
        left: 'auto !important',
        backgroundColor:
          theme.palette.mode === 'light' ? 'rgba(86, 106, 127, 0.25) !important' : 'rgba(203, 203, 226, 0.4) !important'
      },
      '.layout-vertical-nav &': {
        '& .ps__thumb-y': {
          width: 2,
          right: 4,
          backgroundColor: perfectScrollbarThumbBgColor()
        },
        '&:hover, &:focus, &.ps--clicking': {
          backgroundColor: 'transparent !important',
          '& .ps__thumb-y': {
            width: 6
          }
        }
      }
    },
    '#nprogress': {
      pointerEvents: 'none',
      '& .bar': {
        left: 0,
        top: 0,
        height: 3,
        width: '100%',
        zIndex: 2000,
        position: 'fixed',
        backgroundColor: theme.palette.primary.main
      }
    }
  }
}

export default GlobalStyles
