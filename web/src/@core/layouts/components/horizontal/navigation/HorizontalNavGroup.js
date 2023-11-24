/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useState, useEffect, Fragment } from 'react'

import { useRouter } from 'next/router'

import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Fade from '@mui/material/Fade'
import List from '@mui/material/List'
import Paper from '@mui/material/Paper'
import Typography from '@mui/material/Typography'
import ListItemIcon from '@mui/material/ListItemIcon'
import { styled, useTheme } from '@mui/material/styles'
import MuiListItem from '@mui/material/ListItem'

import clsx from 'clsx'
import { usePopper } from 'react-popper'

import Icon from 'src/@core/components/icon'

import themeConfig from 'src/configs/themeConfig'

import HorizontalNavItems from './HorizontalNavItems'
import UserIcon from 'src/layouts/components/UserIcon'
import Translations from 'src/layouts/components/Translations'

import useBgColor from 'src/@core/hooks/useBgColor'

import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'
import { hasActiveChild } from 'src/@core/layouts/utils'

const ListItem = styled(MuiListItem)(({ theme }) => ({
  cursor: 'pointer',
  paddingRight: theme.spacing(3),
  '&:hover': {
    background: theme.palette.action.hover
  }
}))

const NavigationMenu = styled(Paper)(({ theme }) => ({
  overflowY: 'auto',
  padding: theme.spacing(1.25, 0),
  maxHeight: 'calc(100vh - 13rem)',
  backgroundColor: theme.palette.background.paper,
  ...{ width: 250 },
  '&::-webkit-scrollbar': {
    width: 6
  },
  '&::-webkit-scrollbar-thumb': {
    borderRadius: 20,
    background: hexToRGBA(theme.palette.mode === 'light' ? '#B0ACB5' : '#575468', 0.6)
  },
  '&::-webkit-scrollbar-track': {
    borderRadius: 20,
    background: 'transparent'
  },
  '& .MuiList-root': {
    paddingTop: 0,
    paddingBottom: 0
  },
  '& .menu-group.Mui-selected': {
    backgroundColor: theme.palette.action.hover,
    '& .MuiTypography-root, & svg': {
      color: theme.palette.text.primary
    }
  }
}))

const HorizontalNavGroup = props => {
  const { item, hasParent, settings } = props

  const theme = useTheme()
  const router = useRouter()
  const currentURL = router.asPath
  const { mode, skin } = settings
  const bgColors = useBgColor()
  const { navSubItemIcon } = themeConfig
  const popperOffsetHorizontal = -16
  const popperPlacement = 'bottom-start'
  const popperPlacementSubMenu = 'right-start'

  const [menuOpen, setMenuOpen] = useState(false)
  const [popperElement, setPopperElement] = useState(null)
  const [anchorEl, setAnchorEl] = useState(null)
  const [referenceElement, setReferenceElement] = useState(null)

  const { styles, attributes, update } = usePopper(referenceElement, popperElement, {
    placement: hasParent ? popperPlacementSubMenu : popperPlacement,
    modifiers: [
      {
        enabled: true,
        name: 'offset',
        options: {
          offset: hasParent ? [-8, 6] : [popperOffsetHorizontal, 1]
        }
      },
      {
        enabled: true,
        name: 'flip'
      }
    ]
  })

  const handleGroupOpen = event => {
    setAnchorEl(event.currentTarget)
    setMenuOpen(true)
    update ? update() : null
  }

  const handleGroupClose = () => {
    setAnchorEl(null)
    setMenuOpen(false)
  }

  useEffect(() => {
    handleGroupClose()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [router.asPath])
  const icon = item.icon ? item.icon : navSubItemIcon
  const toggleIcon = 'bx:chevron-right'

  const childMenuGroupStyles = () => {
    if (attributes && attributes.popper) {
      if (attributes.popper['data-popper-placement'] === 'right-start') {
        return 'left'
      }
      if (attributes.popper['data-popper-placement'] === 'left-start') {
        return 'right'
      }
    }
  }

  return (
    <div {...{ onMouseLeave: handleGroupClose }}>
      <Fragment>
        <List component='div' sx={{ py: skin === 'bordered' ? 2.375 : 2.5 }}>
          <ListItem
            aria-haspopup='true'
            {...{ onMouseEnter: handleGroupOpen }}
            className={clsx('menu-group', { 'Mui-selected': hasActiveChild(item, currentURL) })}
            sx={{
              ...(menuOpen ? { backgroundColor: 'action.hover' } : {}),
              ...(!hasParent
                ? {
                    borderRadius: 1,
                    '&.Mui-selected': {
                      backgroundColor: mode === 'light' ? bgColors.primaryLight.backgroundColor : 'primary.main',
                      '& .MuiTypography-root, & svg': { color: mode === 'light' ? 'primary.main' : 'common.white' }
                    }
                  }
                : { py: 2.5 })
            }}
          >
            <Box
              sx={{
                gap: 2,
                width: '100%',
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'center',
                justifyContent: 'space-between'
              }}
              ref={setReferenceElement}
            >
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  flexDirection: 'row',
                  overflow: 'hidden'
                }}
              >
                <ListItemIcon
                  sx={{
                    mr: 2,
                    ...(icon === navSubItemIcon && { color: 'text.disabled' }),
                    ...(menuOpen && { color: icon === navSubItemIcon ? 'text.secondary' : 'text.primary' })
                  }}
                >
                  <UserIcon icon={icon} fontSize={icon === navSubItemIcon ? '0.4375rem' : '1.375rem'} />
                </ListItemIcon>
                <Typography noWrap sx={{ ...(!menuOpen && { color: 'text.secondary' }) }}>
                  <Translations text={item.title} />
                </Typography>
              </Box>
              <Box sx={{ display: 'flex', alignItems: 'center', color: menuOpen ? 'text.primary' : 'text.secondary' }}>
                {item.badgeContent ? (
                  <Chip
                    label={item.badgeContent}
                    color={item.badgeColor || 'primary'}
                    sx={{
                      mr: 1.5,
                      height: 20,
                      fontWeight: 500,
                      '& .MuiChip-label': { px: 1.5, textTransform: 'capitalize' }
                    }}
                  />
                ) : null}
                <Icon fontSize='1.25rem' icon={hasParent ? toggleIcon : 'bx:chevron-down'} />
              </Box>
            </Box>
          </ListItem>
          <Fade {...{ in: menuOpen, timeout: { exit: 300, enter: 400 } }}>
            <Box
              style={styles.popper}
              ref={setPopperElement}
              {...attributes.popper}
              sx={{
                zIndex: theme.zIndex.appBar,
                display: menuOpen ? 'block' : 'none',
                pl: childMenuGroupStyles() === 'left' ? (skin === 'bordered' ? 1.5 : 1.25) : 0,
                pr: childMenuGroupStyles() === 'right' ? (skin === 'bordered' ? 1.5 : 1.25) : 0,
                ...(hasParent ? { position: 'fixed !important' } : { pt: skin === 'bordered' ? 5.25 : 5.5 })
              }}
            >
              <NavigationMenu
                sx={{
                  ...(hasParent ? { overflowY: 'auto', overflowX: 'visible', maxHeight: 'calc(100vh - 21rem)' } : {}),
                  ...(skin === 'bordered'
                    ? { boxShadow: 0, border: `1px solid ${theme.palette.divider}` }
                    : { boxShadow: 4 })
                }}
              >
                <HorizontalNavItems {...props} hasParent horizontalNavItems={item.children} />
              </NavigationMenu>
            </Box>
          </Fade>
        </List>
      </Fragment>
    </div>
  )
}

export default HorizontalNavGroup
