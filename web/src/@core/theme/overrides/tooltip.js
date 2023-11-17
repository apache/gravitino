import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const Tooltip = () => {
  return {
    MuiTooltip: {
      defaultProps: {
        arrow: true
      },
      styleOverrides: {
        tooltip: ({ theme }) => ({
          fontWeight: 400,
          borderRadius: 4,
          fontSize: '0.875rem',
          padding: theme.spacing(1, 2.8),
          backgroundColor:
            theme.palette.mode === 'light'
              ? `rgba(${theme.palette.customColors.main}, 0.9)`
              : hexToRGBA(theme.palette.customColors.trackBg, 0.9)
        }),
        arrow: ({ theme }) => ({
          color:
            theme.palette.mode === 'light'
              ? `rgba(${theme.palette.customColors.main}, 0.9)`
              : hexToRGBA(theme.palette.customColors.trackBg, 0.9)
        })
      }
    }
  }
}

export default Tooltip
