/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useTheme } from '@mui/material/styles'

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const CheckedIcon = () => {
  const theme = useTheme()

  return (
    <svg width='24' height='24' viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <path
        fillRule='evenodd'
        clipRule='evenodd'
        fill={theme.palette.primary.main}
        d='M12 3C16.9705 3 21 7.02944 21 12C21 16.9705 16.9705 21 12 21C7.02944 21 3 16.9705 3 12C3 7.02944 7.02944 3 12 3Z'
      />
      <path
        fill={theme.palette.common.white}
        d='M16 12C16 14.2091 14.2091 16 12 16C9.79086 16 8 14.2091 8 12C8 9.79086 9.79086 8 12 8C14.2091 8 16 9.79086 16 12Z'
      />
    </svg>
  )
}

const Icon = () => {
  const theme = useTheme()

  return (
    <svg
      width='24'
      height='24'
      viewBox='0 0 24 24'
      fill='none'
      xmlns='http://www.w3.org/2000/svg'
      stroke={theme.palette.text.disabled}
    >
      <path d='M12 3.5C16.6944 3.5 20.5 7.30558 20.5 12C20.5 16.6944 16.6944 20.5 12 20.5C7.30558 20.5 3.5 16.6944 3.5 12C3.5 7.30558 7.30558 3.5 12 3.5Z' />
    </svg>
  )
}

const Radio = () => {
  return {
    MuiRadio: {
      defaultProps: {
        icon: <Icon />,
        checkedIcon: <CheckedIcon />
      },
      styleOverrides: {
        root: ({ theme }) => ({
          '&:hover': {
            backgroundColor: 'transparent'
          },
          '&.Mui-checked': {
            '& svg': {
              fill: theme.palette.primary.main,
              filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.primary.main, 0.4)})`
            },
            '&.Mui-disabled svg': {
              opacity: 0.4,
              filter: 'none',
              '& path:first-of-type': {
                fill: theme.palette.text.disabled
              },
              '& path:last-of-type': {
                fill: theme.palette.common.white,
                stroke: theme.palette.common.white,
                opacity: theme.palette.mode === 'dark' ? 0.5 : 0.9
              }
            }
          },
          '&.Mui-disabled:not(.Mui-checked) svg': {
            opacity: 0.5
          },
          '&.Mui-checked.MuiRadio-colorSecondary svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.secondary.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.secondary.main}`
            }
          },
          '&.Mui-checked.MuiRadio-colorSuccess svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.success.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.success.main}`
            }
          },
          '&.Mui-checked.MuiRadio-colorError svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.error.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.error.main}`
            }
          },
          '&.Mui-checked.MuiRadio-colorWarning svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.warning.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.warning.main}`
            }
          },
          '&.Mui-checked.MuiRadio-colorInfo svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.info.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.info.main}`
            }
          }
        })
      }
    }
  }
}

export default Radio
