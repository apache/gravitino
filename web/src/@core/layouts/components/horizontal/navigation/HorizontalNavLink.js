import { Fragment } from 'react'

import Link from 'next/link'
import { useRouter } from 'next/router'

import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import List from '@mui/material/List'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import ListItemIcon from '@mui/material/ListItemIcon'
import MuiListItem from '@mui/material/ListItem'

import clsx from 'clsx'

import themeConfig from 'src/configs/themeConfig'

import UserIcon from 'src/layouts/components/UserIcon'
import Translations from 'src/layouts/components/Translations'

import useBgColor from 'src/@core/hooks/useBgColor'

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'
import { handleURLQueries } from 'src/@core/layouts/utils'

const ListItem = styled(MuiListItem)(({ theme }) => ({
  width: 'auto',
  color: theme.palette.text.primary,
  '&:hover': {
    backgroundColor: theme.palette.action.hover
  },
  '&:focus-visible': {
    outline: 0,
    backgroundColor: theme.palette.action.focus
  }
}))

const HorizontalNavLink = props => {
  const { item, settings, hasParent } = props

  const router = useRouter()
  const { skin, mode } = settings
  const bgColors = useBgColor()
  const { navSubItemIcon } = themeConfig
  const icon = item.icon ? item.icon : navSubItemIcon
  const Wrapper = !hasParent ? List : Fragment

  const isNavLinkActive = () => {
    if (router.pathname === item.path || handleURLQueries(router, item.path)) {
      return true
    } else {
      return false
    }
  }

  return (
    <Wrapper {...(!hasParent ? { component: 'div', sx: { py: skin === 'bordered' ? 2.375 : 2.5 } } : {})}>
      <ListItem
        component={Link}
        disabled={item.disabled}
        {...(item.disabled && { tabIndex: -1 })}
        className={clsx({ active: isNavLinkActive() })}
        target={item.openInNewTab ? '_blank' : undefined}
        href={item.path === undefined ? '/' : `${item.path}`}
        onClick={e => {
          if (item.path === undefined) {
            e.preventDefault()
            e.stopPropagation()
          }
        }}
        sx={{
          ...(item.disabled ? { pointerEvents: 'none' } : { cursor: 'pointer' }),
          ...(!hasParent
            ? {
                borderRadius: 1,
                '&.active': {
                  backgroundColor: mode === 'light' ? bgColors.primaryLight.backgroundColor : 'primary.main',
                  '&:focus-visible': {
                    backgroundColor: theme =>
                      mode === 'light' ? hexToRGBA(theme.palette.primary.main, 0.24) : 'primary.dark'
                  },
                  '& .MuiTypography-root': {
                    color: mode === 'light' ? 'primary.main' : 'common.white'
                  }
                }
              }
            : { py: 2.5 })
        }}
      >
        <Box sx={{ gap: 2, width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              overflow: 'hidden',
              ...(hasParent && isNavLinkActive() && { pl: 1.5, ml: -1.5 })
            }}
          >
            <ListItemIcon
              sx={{
                mr: icon === navSubItemIcon ? 2.5 : 2,
                '& svg': { transition: 'transform .25s ease-in-out' },
                ...(icon === navSubItemIcon && { color: 'text.disabled' }),
                ...(isNavLinkActive() && {
                  color: 'primary.main',
                  ...(hasParent &&
                    icon === navSubItemIcon && {
                      '& svg': {
                        transform: 'scale(1.35)',
                        filter: theme => `drop-shadow(0 0 2px ${theme.palette.primary.main})`
                      }
                    })
                })
              }}
            >
              <UserIcon icon={icon} fontSize={icon === navSubItemIcon ? '0.4375rem' : '1.375rem'} />
            </ListItemIcon>
            <Typography
              noWrap
              sx={{ ...(isNavLinkActive() ? hasParent && { fontWeight: 600 } : { color: 'text.secondary' }) }}
            >
              <Translations text={item.title} />
            </Typography>
          </Box>
          {item.badgeContent ? (
            <Chip
              label={item.badgeContent}
              color={item.badgeColor || 'primary'}
              sx={{
                height: 20,
                fontWeight: 500,
                '& .MuiChip-label': { px: 1.5, textTransform: 'capitalize' }
              }}
            />
          ) : null}
        </Box>
      </ListItem>
    </Wrapper>
  )
}

export default HorizontalNavLink
