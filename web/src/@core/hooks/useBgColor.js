import { useTheme } from '@mui/material/styles'

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const UseBgColor = () => {
  const theme = useTheme()

  return {
    primaryFilled: {
      color: theme.palette.primary.contrastText,
      backgroundColor: theme.palette.primary.main
    },
    primaryLight: {
      color: theme.palette.primary.main,
      backgroundColor: hexToRGBA(theme.palette.primary.main, 0.16)
    },
    secondaryFilled: {
      color: theme.palette.secondary.contrastText,
      backgroundColor: theme.palette.secondary.main
    },
    secondaryLight: {
      color: theme.palette.secondary.main,
      backgroundColor: hexToRGBA(theme.palette.secondary.main, 0.16)
    },
    successFilled: {
      color: theme.palette.success.contrastText,
      backgroundColor: theme.palette.success.main
    },
    successLight: {
      color: theme.palette.success.main,
      backgroundColor: hexToRGBA(theme.palette.success.main, 0.16)
    },
    errorFilled: {
      color: theme.palette.error.contrastText,
      backgroundColor: theme.palette.error.main
    },
    errorLight: {
      color: theme.palette.error.main,
      backgroundColor: hexToRGBA(theme.palette.error.main, 0.16)
    },
    warningFilled: {
      color: theme.palette.warning.contrastText,
      backgroundColor: theme.palette.warning.main
    },
    warningLight: {
      color: theme.palette.warning.main,
      backgroundColor: hexToRGBA(theme.palette.warning.main, 0.16)
    },
    infoFilled: {
      color: theme.palette.info.contrastText,
      backgroundColor: theme.palette.info.main
    },
    infoLight: {
      color: theme.palette.info.main,
      backgroundColor: hexToRGBA(theme.palette.info.main, 0.16)
    }
  }
}

export default UseBgColor
