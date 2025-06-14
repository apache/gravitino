/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { createTheme, responsiveFontSizes } from '@mui/material'

import { alpha } from '../utils/color'
import settings from '../settings'
import colors from './colors'
import screens from './screens'

const { customs, primary, secondary, success, error, warning, info, grey } = colors

const createMuiTheme = (config = {}) => {
  const mode = config.mode ?? settings.mode ?? 'light'

  return responsiveFontSizes(
    createTheme({
      palette: {
        mode,
        common: {
          black: customs.black,
          white: customs.white
        },
        primary: {
          ...primary,
          contrastText: customs.white
        },
        secondary: {
          ...secondary,
          contrastText: customs.white
        },
        error: {
          ...error,
          contrastText: customs.white
        },
        warning: {
          ...warning,
          contrastText: customs.white
        },
        info: {
          ...info,
          contrastText: customs.white
        },
        success: {
          ...success,
          contrastText: customs.white
        },
        grey: {
          ...grey
        },
        text: {
          primary: alpha(customs.main, 0.87),
          secondary: alpha(customs.main, 0.6),
          disabled: alpha(customs.main, 0.38)
        },
        divider: alpha(customs.main, 0.12),
        background: {
          paper: mode === 'light' ? customs.white : customs.darkBg,
          default: mode === 'light' ? customs.lightBg : customs.darkBg
        },
        action: {
          active: alpha(customs.main, 0.54),
          hover: alpha(customs.main, 0.04),
          selected: alpha(customs.main, 0.08),
          disabled: alpha(customs.main, 0.26),
          disabledBackground: alpha(customs.main, 0.12),
          focus: alpha(customs.main, 0.12)
        }
      },
      breakpoints: {
        values: {
          ...screens
        }
      },
      spacing: factor => `${0.25 * factor}rem`,
      shape: {
        borderRadius: 6
      },
      mixins: {
        toolbar: {
          minHeight: 64
        }
      },
      shadows: [
        'none',
        `0px 2px 1px -1px ${alpha(customs.main, 0.2)}, 0px 1px 1px 0px ${alpha(
          customs.main,
          0.14
        )}, 0px 1px 3px 0px ${alpha(customs.main, 0.12)}`,
        `0px 3px 1px -2px ${alpha(customs.main, 0.2)}, 0px 2px 2px 0px ${alpha(
          customs.main,
          0.14
        )}, 0px 1px 5px 0px ${alpha(customs.main, 0.12)}`,
        `0px 4px 8px -4px ${alpha(customs.main, 0.42)}`,
        `0px 6px 18px -8px ${alpha(customs.main, 0.56)}`,
        `0px 3px 5px -1px ${alpha(customs.main, 0.2)}, 0px 5px 8px 0px ${alpha(
          customs.main,
          0.14
        )}, 0px 1px 14px 0px ${alpha(customs.main, 0.12)}`,
        `0px 2px 10px 0px ${alpha(customs.main, 0.1)}`,
        `0px 4px 5px -2px ${alpha(customs.main, 0.2)}, 0px 7px 10px 1px ${alpha(
          customs.main,
          0.14
        )}, 0px 2px 16px 1px ${alpha(customs.main, 0.12)}`,
        `0px 5px 5px -3px ${alpha(customs.main, 0.2)}, 0px 8px 10px 1px ${alpha(
          customs.main,
          0.14
        )}, 0px 3px 14px 2px ${alpha(customs.main, 0.12)}`,
        `0px 5px 6px -3px ${alpha(customs.main, 0.2)}, 0px 9px 12px 1px ${alpha(
          customs.main,
          0.14
        )}, 0px 3px 16px 2px ${alpha(customs.main, 0.12)}`,
        `0px 6px 6px -3px ${alpha(customs.main, 0.2)}, 0px 10px 14px 1px ${alpha(
          customs.main,
          0.14
        )}, 0px 4px 18px 3px ${alpha(customs.main, 0.12)}`,
        `0px 6px 7px -4px ${alpha(customs.main, 0.2)}, 0px 11px 15px 1px ${alpha(
          customs.main,
          0.14
        )}, 0px 4px 20px 3px ${alpha(customs.main, 0.12)}`,
        `0px 7px 8px -4px ${alpha(customs.main, 0.2)}, 0px 12px 17px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 5px 22px 4px ${alpha(customs.main, 0.12)}`,
        `0px 7px 8px -4px ${alpha(customs.main, 0.2)}, 0px 13px 19px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 5px 24px 4px ${alpha(customs.main, 0.12)}`,
        `0px 7px 9px -4px ${alpha(customs.main, 0.2)}, 0px 14px 21px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 5px 26px 4px ${alpha(customs.main, 0.12)}`,
        `0px 8px 9px -5px ${alpha(customs.main, 0.2)}, 0px 15px 22px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 6px 28px 5px ${alpha(customs.main, 0.12)}`,
        `0px 8px 10px -5px ${alpha(customs.main, 0.2)}, 0px 16px 24px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 6px 30px 5px ${alpha(customs.main, 0.12)}`,
        `0px 8px 11px -5px ${alpha(customs.main, 0.2)}, 0px 17px 26px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 6px 32px 5px ${alpha(customs.main, 0.12)}`,
        `0px 9px 11px -5px ${alpha(customs.main, 0.2)}, 0px 18px 28px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 7px 34px 6px ${alpha(customs.main, 0.12)}`,
        `0px 9px 12px -6px ${alpha(customs.main, 0.2)}, 0px 19px 29px 2px ${alpha(
          customs.main,
          0.14
        )}, 0px 7px 36px 6px ${alpha(customs.main, 0.12)}`,
        `0px 10px 13px -6px ${alpha(customs.main, 0.2)}, 0px 20px 31px 3px ${alpha(
          customs.main,
          0.14
        )}, 0px 8px 38px 7px ${alpha(customs.main, 0.12)}`,
        `0px 10px 13px -6px ${alpha(customs.main, 0.2)}, 0px 21px 33px 3px ${alpha(
          customs.main,
          0.14
        )}, 0px 8px 40px 7px ${alpha(customs.main, 0.12)}`,
        `0px 10px 14px -6px ${alpha(customs.main, 0.2)}, 0px 22px 35px 3px ${alpha(
          customs.main,
          0.14
        )}, 0px 8px 42px 7px ${alpha(customs.main, 0.12)}`,
        `0px 11px 14px -7px ${alpha(customs.main, 0.2)}, 0px 23px 36px 3px ${alpha(
          customs.main,
          0.14
        )}, 0px 9px 44px 8px ${alpha(customs.main, 0.12)}`,
        `0px 11px 15px -7px ${alpha(customs.main, 0.2)}, 0px 24px 38px 3px ${alpha(
          customs.main,
          0.14
        )}, 0px 9px 46px 8px ${alpha(customs.main, 0.12)}`
      ],
      typography: {
        fontFamily: [
          'Public Sans',
          'sans-serif',
          '-apple-system',
          'BlinkMacSystemFont',
          '"Segoe UI"',
          'Roboto',
          '"Helvetica Neue"',
          'Arial',
          'sans-serif',
          '"Apple Color Emoji"',
          '"Segoe UI Emoji"',
          '"Segoe UI Symbol"'
        ].join(','),
        h1: {
          fontWeight: 500
        },
        h2: {
          fontWeight: 500
        },
        h3: {
          fontWeight: 500
        },
        h4: {
          fontWeight: 500
        },
        h5: {
          fontWeight: 500
        }
      },
      components: {
        MuiButton: {
          styleOverrides: {
            root: ({ ownerState, theme }) => ({
              fontWeight: 500,
              lineHeight: '24px',
              letterSpacing: '0.3px',
              ...(ownerState.size === 'medium' &&
                ownerState.variant === 'text' && {
                  padding: theme.spacing(1.75, 5)
                }),
              '&:not(.MuiButtonGroup-grouped)': {
                transition: 'all 0.2s ease-in-out',
                '&:hover': {
                  transform: 'translateY(-1px)'
                }
              }
            }),
            contained: ({ theme }) => ({
              boxShadow: theme.shadows[3],
              padding: theme.spacing(1.75, 5)
            }),
            containedPrimary: ({ theme }) => ({
              '&:not(.Mui-disabled), &.MuiButtonGroup-grouped:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            containedSecondary: ({ theme }) => ({
              '&:not(.Mui-disabled), &.MuiButtonGroup-grouped:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            containedSuccess: ({ theme }) => ({
              '&:not(.Mui-disabled), &.MuiButtonGroup-grouped:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            containedError: ({ theme }) => ({
              '&:not(.Mui-disabled), &.MuiButtonGroup-grouped:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            containedWarning: ({ theme }) => ({
              '&:not(.Mui-disabled), &.MuiButtonGroup-grouped:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            containedInfo: ({ theme }) => ({
              '&:not(.Mui-disabled), &.MuiButtonGroup-grouped:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            outlined: ({ theme }) => ({
              padding: theme.spacing(1.5, 4.75)
            }),
            sizeSmall: ({ theme, ownerState }) => ({
              borderRadius: 4,
              ...(ownerState.variant === 'text' && {
                padding: theme.spacing(1, 3.5)
              }),
              ...(ownerState.variant === 'contained' && {
                padding: theme.spacing(1, 3.5)
              }),
              ...(ownerState.variant === 'outlined' && {
                padding: theme.spacing(0.75, 3.25)
              })
            }),
            sizeLarge: ({ theme, ownerState }) => ({
              borderRadius: 8,
              ...(ownerState.variant === 'text' && {
                padding: theme.spacing(2, 6.5)
              }),
              ...(ownerState.variant === 'contained' && {
                padding: theme.spacing(2, 6.5)
              }),
              ...(ownerState.variant === 'outlined' && {
                padding: theme.spacing(1.75, 6.25)
              })
            })
          }
        },
        MuiButtonBase: {
          defaultProps: {
            disableRipple: false
          }
        },
        MuiFab: {
          styleOverrides: {
            root: ({ theme }) => ({
              '&.MuiFab-success:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(success.main, 0.4)}`
              },
              '&.Mui-error:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(error.main, 0.4)}`
              },
              '&.MuiFab-warning:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(warning.main, 0.4)}`
              },
              '&.MuiFab-info:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(info.main, 0.4)}`
              }
            }),
            primary: ({ theme }) => ({
              '&:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
              }
            }),
            secondary: ({ theme }) => ({
              '&:not(.Mui-disabled)': {
                boxShadow: `0 2px 4px 0 ${alpha(secondary.main, 0.4)}`
              }
            })
          }
        },
        MuiButtonGroup: {
          styleOverrides: {
            contained: ({ theme }) => ({
              boxShadow: 'none',
              '& .MuiButton-contained': {
                paddingLeft: theme.spacing(5),
                paddingRight: theme.spacing(5),
                '&.MuiButton-containedPrimary:hover': {
                  boxShadow: `0 2px 4px 0 ${alpha(primary.main, 0.4)}`
                },
                '&.MuiButton-containedSecondary:hover': {
                  boxShadow: `0 2px 4px 0 ${alpha(secondary.main, 0.4)}`
                },
                '&.MuiButton-containedSuccess:hover': {
                  boxShadow: `0 2px 4px 0 ${alpha(success.main, 0.4)}`
                },
                '&.MuiButton-containedError:hover': {
                  boxShadow: `0 2px 4px 0 ${alpha(error.main, 0.4)}`
                },
                '&.MuiButton-containedWarning:hover': {
                  boxShadow: `0 2px 4px 0 ${alpha(warning.main, 0.4)}`
                },
                '&.MuiButton-containedInfo:hover': {
                  boxShadow: `0 2px 4px 0 ${alpha(info.main, 0.4)}`
                }
              }
            })
          }
        },
        MuiLink: {
          styleOverrides: {
            root: {
              textDecoration: 'none'
            }
          }
        },
        MuiTypography: {
          styleOverrides: {
            gutterBottom: ({ theme }) => ({
              marginBottom: theme.spacing(2)
            })
          },
          variants: [
            {
              props: { variant: 'h1' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'h2' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'h3' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'h4' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'h5' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'h6' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'subtitle1' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'subtitle2' },
              style: ({ theme }) => ({ color: theme.palette.text.secondary })
            },
            {
              props: { variant: 'body1' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'body2' },
              style: ({ theme }) => ({ color: theme.palette.text.secondary })
            },
            {
              props: { variant: 'button' },
              style: ({ theme }) => ({ color: theme.palette.text.primary })
            },
            {
              props: { variant: 'caption' },
              style: ({ theme }) => ({ color: theme.palette.text.secondary })
            },
            {
              props: { variant: 'overline' },
              style: ({ theme }) => ({ color: theme.palette.text.secondary })
            }
          ]
        },
        MuiPaper: {
          styleOverrides: {
            root: {
              backgroundImage: 'none'
            }
          }
        },
        MuiBackdrop: {
          styleOverrides: {
            root: ({ theme }) => ({
              backgroundColor: `${alpha(customs.main, 0.5)}`
            }),
            invisible: {
              backgroundColor: 'transparent'
            }
          }
        },
        MuiSelect: {
          styleOverrides: {
            select: {
              minWidth: '6rem !important',
              '&.MuiTablePagination-select': {
                minWidth: '1.5rem !important'
              }
            }
          }
        },
        MuiDialog: {
          styleOverrides: {
            paper: ({ theme }) => ({
              boxShadow: theme.shadows[6],
              border: `1px solid ${theme.palette.divider}`,
              '&:not(.MuiDialog-paperFullScreen)': {
                borderRadius: 8,
                [theme.breakpoints.down('sm')]: {
                  margin: theme.spacing(4),
                  width: `calc(100% - ${theme.spacing(8)})`,
                  maxWidth: `calc(100% - ${theme.spacing(8)}) !important`
                }
              },
              '& > .MuiList-root': {
                paddingLeft: theme.spacing(1),
                paddingRight: theme.spacing(1)
              }
            })
          }
        },
        MuiDialogTitle: {
          styleOverrides: {
            root: ({ theme }) => ({
              padding: theme.spacing(6, 6, 1),
              '& + .MuiDialogContent-root': {
                paddingTop: `${theme.spacing(6)} !important`
              }
            })
          }
        },
        MuiDialogContent: {
          styleOverrides: {
            root: ({ theme }) => ({
              padding: theme.spacing(6),
              '& + .MuiDialogContent-root': {
                paddingTop: '0 !important'
              },
              '& + .MuiDialogActions-root': {
                paddingTop: '0 !important'
              }
            })
          }
        },
        MuiDialogActions: {
          styleOverrides: {
            root: ({ theme }) => ({
              justifyContent: 'center',
              padding: theme.spacing(1, 6, 6),
              '&.dialog-actions-dense': {
                padding: theme.spacing(3),
                paddingTop: theme.spacing(1)
              }
            })
          }
        },

        MuiCard: {
          styleOverrides: {
            root: ({ theme }) => ({
              borderRadius: 8,
              '& .card-more-options': {
                marginTop: theme.spacing(-1),
                marginRight: theme.spacing(-3)
              },
              '& .MuiTableContainer-root, & .MuiDataGrid-root, & .MuiDataGrid-columnHeaders': {
                borderRadius: 0
              }
            })
          },
          defaultProps: {
            elevation: 6
          }
        },
        MuiCardHeader: {
          styleOverrides: {
            root: ({ theme }) => ({
              padding: theme.spacing(6),
              '& + .MuiCardContent-root, & + .MuiCardActions-root, & + .MuiCollapse-root .MuiCardContent-root': {
                paddingTop: 0
              },
              '& .MuiCardHeader-subheader': {
                fontSize: '0.875rem',
                marginTop: theme.spacing(1.25),
                color: theme.palette.text.secondary
              }
            }),
            title: {
              lineHeight: 1.6,
              fontWeight: 500,
              fontSize: '1.125rem',
              letterSpacing: '0.15px',
              '@media (min-width: 600px)': {
                fontSize: '1.25rem'
              }
            },
            action: {
              marginTop: 0,
              marginRight: 0
            }
          }
        },
        MuiCardContent: {
          styleOverrides: {
            root: ({ theme }) => ({
              padding: theme.spacing(6),
              '& + .MuiCardHeader-root, & + .MuiCardContent-root, & + .MuiCardActions-root': {
                paddingTop: 0
              },
              '&:last-of-type': {
                paddingBottom: theme.spacing(6)
              }
            })
          }
        },
        MuiCardActions: {
          styleOverrides: {
            root: ({ theme }) => ({
              padding: theme.spacing(6),
              '& .MuiButton-text': {
                paddingLeft: theme.spacing(3),
                paddingRight: theme.spacing(3)
              },
              '&.card-action-dense': {
                padding: theme.spacing(0, 3, 3),
                '.MuiCard-root .MuiCardMedia-root + &': {
                  paddingTop: theme.spacing(3)
                }
              },
              '.MuiCard-root &:first-of-type': {
                paddingTop: theme.spacing(3),
                '& + .MuiCardHeader-root, & + .MuiCardContent-root, & + .MuiCardActions-root': {
                  paddingTop: 0
                }
              }
            })
          }
        },
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
                borderBottom: `1px solid ${alpha(customs.main, 0.2)}`
              },
              '&:hover:not(.Mui-disabled):before': {
                borderBottom: `1px solid ${alpha(customs.main, 0.32)}`
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
              backgroundColor: alpha(customs.main, 0.04),
              '&:hover:not(.Mui-disabled)': {
                backgroundColor: alpha(customs.main, 0.08)
              },
              '&:before': {
                borderBottom: `1px solid ${alpha(customs.main, 0.22)}`
              },
              '&:hover:not(.Mui-disabled):before': {
                borderBottom: `1px solid ${alpha(customs.main, 0.32)}`
              }
            })
          }
        },
        MuiOutlinedInput: {
          styleOverrides: {
            root: ({ theme }) => ({
              '&:hover:not(.Mui-focused):not(.Mui-disabled):not(.Mui-error) .MuiOutlinedInput-notchedOutline': {
                borderColor: alpha(customs.main, 0.32)
              },
              '&:hover.Mui-error .MuiOutlinedInput-notchedOutline': {
                borderColor: error.main
              },
              '&.Mui-error.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(error.main, 0.4)} !important`
              },
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: alpha(customs.main, 0.22)
              },
              '&.Mui-disabled .MuiOutlinedInput-notchedOutline': {
                borderColor: theme.palette.text.disabled
              },
              '&.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(primary.main, 0.4)}`
              },
              '&.MuiInputBase-colorSuccess.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(success.main, 0.4)}`
              },
              '&.MuiInputBase-colorWarning.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(warning.main, 0.4)}`
              },
              '&.MuiInputBase-colorError.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(error.main, 0.4)}`
              },
              '&.MuiInputBase-colorInfo.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(info.main, 0.4)}`
              }
            }),
            colorSecondary: ({ theme }) => ({
              '&.Mui-focused': {
                boxShadow: `0 1px 3px 0 ${alpha(secondary.main, 0.4)}`
              }
            })
          }
        },
        MuiDataGrid: {
          styleOverrides: {
            root: ({ theme }) => ({
              border: 0,
              color: theme.palette.text.primary,
              '& .MuiDataGrid-columnHeader:focus, & .MuiDataGrid-columnHeader:focus-within': {
                outline: 'none'
              }
            }),
            toolbarContainer: ({ theme }) => ({
              paddingRight: `${theme.spacing(5)} !important`,
              paddingLeft: `${theme.spacing(3.25)} !important`
            }),
            columnHeaders: ({ theme }) => ({
              borderTop: `1px solid ${theme.palette.divider}`,
              backgroundColor: theme.palette.action.hover
            }),
            columnHeader: ({ theme }) => ({
              '&:not(.MuiDataGrid-columnHeaderCheckbox)': {
                paddingLeft: theme.spacing(4),
                paddingRight: theme.spacing(4),
                '&:first-of-type': {
                  paddingLeft: theme.spacing(5)
                }
              },
              '&:last-of-type': {
                paddingRight: theme.spacing(5)
              }
            }),
            columnHeaderCheckbox: {
              maxWidth: '58px !important',
              minWidth: '58px !important'
            },
            columnHeaderTitleContainer: {
              padding: 0
            },
            columnHeaderTitle: {
              fontWeight: 600,
              fontSize: '0.75rem',
              letterSpacing: '0.17px'
            },
            columnSeparator: ({ theme }) => ({
              color: theme.palette.divider
            }),
            row: ({ theme }) => ({
              // Disable hover effects
              '&:hover': {
                backgroundColor: 'transparent !important'
              },
              '&.Mui-hovered': {
                backgroundColor: 'transparent !important'
              },
              '&:last-child': {
                '& .MuiDataGrid-cell': {
                  borderBottom: 0
                }
              }
            }),
            cell: ({ theme }) => ({
              borderColor: theme.palette.divider,
              '&:not(.MuiDataGrid-cellCheckbox)': {
                paddingLeft: theme.spacing(4),
                paddingRight: theme.spacing(4),
                '&:first-of-type': {
                  paddingLeft: theme.spacing(5)
                }
              },
              '&:last-of-type': {
                paddingRight: theme.spacing(5)
              },
              '&:focus, &:focus-within': {
                outline: 'none'
              }
            }),
            cellCheckbox: {
              maxWidth: '58px !important',
              minWidth: '58px !important'
            },
            editInputCell: ({ theme }) => ({
              padding: 0,
              color: theme.palette.text.primary,
              '& .MuiInputBase-input': {
                padding: 0
              }
            }),
            footerContainer: ({ theme }) => ({
              borderTop: `1px solid ${theme.palette.divider}`,
              '& .MuiTablePagination-toolbar': {
                paddingLeft: `${theme.spacing(4)} !important`,
                paddingRight: `${theme.spacing(4)} !important`
              },
              '& .MuiTablePagination-displayedRows, & .MuiTablePagination-selectLabel': {
                color: theme.palette.text.primary
              }
            }),
            selectedRowCount: ({ theme }) => ({
              margin: 0,
              paddingLeft: theme.spacing(4),
              paddingRight: theme.spacing(4)
            })
          }
        },
        MuiBreadcrumbs: {
          styleOverrides: {
            root: ({ theme }) => ({
              '& a': {
                textDecoration: 'none',
                color: primary.main
              }
            }),
            li: ({ theme }) => ({
              color: theme.palette.text.secondary,
              '& .MuiTypography-root': {
                color: 'inherit'
              }
            })
          }
        }
      }
    })
  )
}

export default createMuiTheme
