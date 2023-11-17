import { useTheme } from '@mui/material/styles'

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const CheckedIcon = () => {
  const theme = useTheme()

  return (
    <svg width='24' height='24' viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <path
        fill={theme.palette.primary.main}
        d='M17.5514 3H6.75137C4.76314 3 3.15137 4.61177 3.15137 6.6V17.4C3.15137 19.3882 4.76314 21 6.75137 21H17.5514C19.5396 21 21.1514 19.3882 21.1514 17.4V6.6C21.1514 4.61177 19.5396 3 17.5514 3Z'
      />
      <path
        strokeWidth='0.3'
        fill={theme.palette.common.white}
        stroke={theme.palette.common.white}
        d='M8.63616 11.5663L8.53426 11.472L8.43235 11.5663L7.54946 12.3837L7.43057 12.4938L7.54946 12.6039L10.4885 15.3248L10.5904 15.4191L10.6923 15.3248L16.7533 9.71358L16.8722 9.60351L16.7533 9.49344L15.8704 8.67606L15.7685 8.58172L15.6666 8.67606L10.5904 13.3756L8.63616 11.5663Z'
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
      fill='none'
      viewBox='0 0 24 24'
      xmlns='http://www.w3.org/2000/svg'
      stroke={theme.palette.text.disabled}
    >
      <path d='M17.2105 3.5H6.78947C4.69661 3.5 3 5.19661 3 7.28947V17.7105C3 19.8034 4.69661 21.5 6.78947 21.5H17.2105C19.3034 21.5 21 19.8034 21 17.7105V7.28947C21 5.19661 19.3034 3.5 17.2105 3.5Z' />
    </svg>
  )
}

const IndeterminateIcon = () => {
  const theme = useTheme()

  return (
    <svg
      width='24'
      height='24'
      viewBox='0 0 24 24'
      fill={theme.palette.primary.main}
      xmlns='http://www.w3.org/2000/svg'
    >
      <path
        fillRule='evenodd'
        clipRule='evenodd'
        d='M6.6 3.5H17.4C19.3882 3.5 21 5.11177 21 7.1V17.9C21 19.8882 19.3882 21.5 17.4 21.5H6.6C4.61177 21.5 3 19.8882 3 17.9V7.1C3 5.11177 4.61177 3.5 6.6 3.5ZM16.5 11.8569H7.5V13.1426H16.5V11.8569Z'
      />
    </svg>
  )
}

const Checkbox = () => {
  return {
    MuiCheckbox: {
      defaultProps: {
        icon: <Icon />,
        checkedIcon: <CheckedIcon />,
        indeterminateIcon: <IndeterminateIcon />
      },
      styleOverrides: {
        root: ({ theme }) => ({
          '&.Mui-checked': {
            '& svg': {
              filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.primary.main, 0.4)})`
            },
            '&.Mui-disabled': {
              '& svg': {
                filter: 'none',
                '& path:first-of-type': {
                  fill: theme.palette.action.disabled
                },
                '& path:last-of-type': {
                  fill: theme.palette.background.paper,
                  stroke: theme.palette.background.paper
                }
              }
            }
          },
          '&.Mui-disabled:not(.Mui-checked) svg': {
            stroke: theme.palette.action.disabled
          },
          '&.Mui-checked.MuiCheckbox-colorSecondary svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.secondary.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.secondary.main}`
            }
          },
          '&.Mui-checked.MuiCheckbox-colorSuccess svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.success.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.success.main}`
            }
          },
          '&.Mui-checked.MuiCheckbox-colorError svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.error.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.error.main}`
            }
          },
          '&.Mui-checked.MuiCheckbox-colorWarning svg': {
            filter: `drop-shadow(0 2px 4px ${hexToRGBA(theme.palette.warning.main, 0.4)})`,
            '& path:first-of-type': {
              fill: `${theme.palette.warning.main}`
            }
          },
          '&.Mui-checked.MuiCheckbox-colorInfo svg': {
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

export default Checkbox
