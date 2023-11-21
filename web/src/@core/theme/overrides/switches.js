/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const Switch = () => {
  return {
    MuiSwitch: {
      defaultProps: {
        disableRipple: true
      },
      styleOverrides: {
        root: ({ theme }) => ({
          width: 54,
          height: 42,
          '& .MuiSwitch-track': {
            width: 30,
            height: 18,
            opacity: 1,
            borderRadius: 30,
            backgroundColor: 'transparent',
            border: `1px solid ${theme.palette.text.disabled}`,
            transition: 'left .15s ease-in-out, transform .15s ease-in-out'
          }
        }),
        switchBase: ({ theme }) => ({
          top: 12,
          left: 12,
          padding: `${theme.spacing(0.75)} !important`,
          color: `rgba(${theme.palette.customColors.main}, 0.6)`,
          transition: 'left .15s ease-in-out, transform .15s ease-in-out',
          '&:hover': {
            backgroundColor: 'transparent !important'
          },
          '& .MuiSwitch-input': {
            left: '-50%',
            width: '250%'
          },
          '&.Mui-disabled': {
            opacity: 0.4,
            color: theme.palette.text.disabled,
            '& + .MuiSwitch-track': {
              opacity: 0.5
            },
            '&.Mui-checked': {
              opacity: theme.palette.mode === 'dark' ? 0.5 : 0.9,
              '& + .MuiSwitch-track': {
                opacity: 0.3,
                boxShadow: 'none'
              }
            }
          },
          '&.Mui-checked': {
            left: 4,
            color: `${theme.palette.common.white} !important`,
            '& .MuiSwitch-input': {
              left: '-100%'
            },
            '& + .MuiSwitch-track': {
              opacity: 1,
              borderColor: theme.palette.primary.main,
              backgroundColor: theme.palette.primary.main,
              boxShadow: `0 2px 4px 0 ${hexToRGBA(theme.palette.primary.main, 0.4)}`
            },
            '&.MuiSwitch-colorSecondary + .MuiSwitch-track': {
              borderColor: theme.palette.secondary.main,
              backgroundColor: theme.palette.secondary.main,
              boxShadow: `0 2px 4px 0 ${hexToRGBA(theme.palette.secondary.main, 0.4)}`
            },
            '&.MuiSwitch-colorSuccess + .MuiSwitch-track': {
              borderColor: theme.palette.success.main,
              backgroundColor: theme.palette.success.main,
              boxShadow: `0 2px 4px 0 ${hexToRGBA(theme.palette.success.main, 0.4)}`
            },
            '&.MuiSwitch-colorError + .MuiSwitch-track': {
              borderColor: theme.palette.error.main,
              backgroundColor: theme.palette.error.main,
              boxShadow: `0 2px 4px 0 ${hexToRGBA(theme.palette.error.main, 0.4)}`
            },
            '&.MuiSwitch-colorWarning + .MuiSwitch-track': {
              borderColor: theme.palette.warning.main,
              backgroundColor: theme.palette.warning.main,
              boxShadow: `0 2px 4px 0 ${hexToRGBA(theme.palette.warning.main, 0.4)}`
            },
            '&.MuiSwitch-colorInfo + .MuiSwitch-track': {
              borderColor: theme.palette.info.main,
              backgroundColor: theme.palette.info.main,
              boxShadow: `0 2px 4px 0 ${hexToRGBA(theme.palette.info.main, 0.4)}`
            }
          }
        }),
        thumb: {
          width: 12,
          height: 12,
          boxShadow: 'none'
        },
        sizeSmall: {
          width: 38,
          height: 30,
          '& .MuiSwitch-track': {
            width: 24,
            height: 16
          },
          '& .MuiSwitch-thumb': {
            width: 10,
            height: 10
          },
          '& .MuiSwitch-switchBase': {
            top: 7,
            left: 7,
            '&.Mui-checked': {
              left: -1
            }
          }
        }
      }
    }
  }
}

export default Switch
