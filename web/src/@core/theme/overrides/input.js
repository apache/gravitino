// ** Util Import
import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const input = () => {
  return {
    MuiInputLabel: {
      styleOverrides: {
        root: ({ theme }) => ({
          color: theme.palette.text.secondary
        })
      }
    },
    MuiInput: {
      styleOverrides: {
        root: ({ theme }) => ({
          '&:before': {
            borderBottom: `1px solid rgba(${theme.palette.customColors.main}, 0.22)`
          },
          '&:hover:not(.Mui-disabled):before': {
            borderBottom: `1px solid rgba(${theme.palette.customColors.main}, 0.32)`
          },
          '&.Mui-disabled:before': {
            borderBottomStyle: 'solid'
          }
        })
      }
    },
    MuiFilledInput: {
      styleOverrides: {
        root: ({ theme }) => ({
          backgroundColor: `rgba(${theme.palette.customColors.main}, 0.04)`,
          '&:hover:not(.Mui-disabled)': {
            backgroundColor: `rgba(${theme.palette.customColors.main}, 0.08)`
          },
          '&:before': {
            borderBottom: `1px solid rgba(${theme.palette.customColors.main}, 0.22)`
          },
          '&:hover:not(.Mui-disabled):before': {
            borderBottom: `1px solid rgba(${theme.palette.customColors.main}, 0.32)`
          }
        })
      }
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: ({ theme }) => ({
          '&:hover:not(.Mui-focused):not(.Mui-disabled):not(.Mui-error) .MuiOutlinedInput-notchedOutline': {
            borderColor: `rgba(${theme.palette.customColors.main}, 0.32)`
          },
          '&:hover.Mui-error .MuiOutlinedInput-notchedOutline': {
            borderColor: theme.palette.error.main
          },
          '&.Mui-error.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.error.main, 0.4)} !important`
          },
          '& .MuiOutlinedInput-notchedOutline': {
            borderColor: `rgba(${theme.palette.customColors.main}, 0.22)`
          },
          '&.Mui-disabled .MuiOutlinedInput-notchedOutline': {
            borderColor: theme.palette.text.disabled
          },
          '&.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.primary.main, 0.4)}`
          },
          '&.MuiInputBase-colorSuccess.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.success.main, 0.4)}`
          },
          '&.MuiInputBase-colorWarning.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.warning.main, 0.4)}`
          },
          '&.MuiInputBase-colorError.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.error.main, 0.4)}`
          },
          '&.MuiInputBase-colorInfo.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.info.main, 0.4)}`
          }
        }),
        colorSecondary: ({ theme }) => ({
          '&.Mui-focused': {
            boxShadow: `0 1px 3px 0 ${hexToRGBA(theme.palette.secondary.main, 0.4)}`
          }
        })
      }
    }
  }
}

export default input
