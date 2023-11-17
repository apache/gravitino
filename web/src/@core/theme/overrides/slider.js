import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const Slider = () => {
  return {
    MuiSlider: {
      styleOverrides: {
        root: {
          borderRadius: 8,
          '&:not(.MuiSlider-vertical)': {
            height: 6
          },
          '&.MuiSlider-vertical': {
            width: 6
          }
        },
        thumb: ({ theme }) => ({
          width: 14,
          height: 14,
          boxShadow: theme.shadows[3],
          '&:before': {
            border: `4px solid ${
              theme.palette.mode === 'dark' ? theme.palette.background.default : theme.palette.background.paper
            }`
          },
          '&:not(.Mui-active):after': {
            width: 30,
            height: 30
          },
          '&.Mui-active': {
            width: 18,
            height: 18,
            boxShadow: `0 0 0 10px ${hexToRGBA(theme.palette.primary.main, 0.16)}`,
            '&:before': {
              borderWidth: 5
            },
            '&:after': {
              width: 38,
              height: 38
            }
          }
        }),
        sizeSmall: ({ theme }) => ({
          '&:not(.MuiSlider-vertical)': {
            height: 4
          },
          '&.MuiSlider-vertical': {
            width: 4
          },
          '& .MuiSlider-thumb.Mui-focusVisible': {
            boxShadow: `0 0 0 6px ${hexToRGBA(theme.palette.primary.main, 0.16)}`
          }
        }),
        thumbSizeSmall: ({ theme }) => ({
          width: 12,
          height: 12,
          '&:hover': {
            boxShadow: `0 0 0 6px ${hexToRGBA(theme.palette.primary.main, 0.16)}`
          },
          '&:before': {
            boxShadow: theme.shadows[2]
          },
          '&:not(.Mui-active):after': {
            width: 24,
            height: 24
          },
          '&.Mui-active': {
            width: 14,
            height: 14,
            boxShadow: `0 0 0 8px ${hexToRGBA(theme.palette.primary.main, 0.16)} !important`,
            '&:before': {
              borderWidth: 4
            },
            '&:after': {
              width: 30,
              height: 30
            }
          }
        })
      }
    }
  }
}

export default Slider
