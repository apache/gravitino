/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { forwardRef } from 'react'

import Link from 'next/link'

import { Box, styled, Typography, Skeleton } from '@mui/material'
import { TreeView } from '@mui/x-tree-view/TreeView'
import { TreeItem, useTreeItem } from '@mui/x-tree-view/TreeItem'

import Icon from '@/components/Icon'

import clsx from 'clsx'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { setExpandedTreeNode, setIntoTreeAction, resetTableData } from '@/lib/store/metalakes'

const StyledLink = styled(Link)(({ theme }) => ({
  textDecoration: 'none'
}))

const CustomContent = forwardRef(function CustomContent(props, ref) {
  const dispatch = useAppDispatch()
  const { classes, className, label, nodeId, icon: iconProp, expansionIcon, displayIcon } = props

  const { disabled, expanded, selected, focused, handleExpansion, handleSelection, preventSelection } =
    useTreeItem(nodeId)

  const icon = iconProp || expansionIcon || displayIcon

  const handleMouseDown = event => {
    preventSelection(event)
  }

  const handleExpansionClick = event => {
    handleExpansion(event)
  }

  const handleSelectionClick = event => {
    dispatch(resetTableData())
    handleSelection(event)
  }

  return (
    <div
      className={clsx(className, classes.root, {
        [classes.expanded]: expanded,
        [classes.selected]: selected,
        [classes.focused]: focused,
        [classes.disabled]: disabled
      })}
      onMouseDown={handleMouseDown}
      ref={ref}
    >
      <div onClick={handleExpansionClick} className={classes.iconContainer} data-node-id={nodeId}>
        {icon}
      </div>
      <Typography onClick={handleSelectionClick} component='div' className={classes.label}>
        {label}
      </Typography>
    </div>
  )
})

const StyledTreeItemRoot = styled(TreeItem)(({ theme, level = 0 }) => ({
  '&:hover > .MuiTreeItem-content:not(.Mui-selected)': {
    backgroundColor: theme.palette.action.hover
  },
  '& .MuiTreeItem-content': {
    paddingRight: theme.spacing(3),
    fontWeight: theme.typography.fontWeightMedium
  },
  '& .MuiTreeItem-label': {
    fontWeight: 'inherit',
    paddingRight: theme.spacing(3)
  },
  '& .MuiTreeItem-group': {
    marginLeft: 0,
    '& .MuiTreeItem-content': {
      paddingLeft: theme.spacing(4 + level * 4),
      fontWeight: theme.typography.fontWeightRegular
    }
  }
}))

const StyledTreeItem = props => {
  const { labelText, labelIcon, labelInfo, href, dispatch, ...other } = props

  return (
    <StyledTreeItemRoot
      {...other}
      label={
        <Box sx={{ py: 1, display: 'flex', alignItems: 'center', '& svg': { mr: 1 } }}>
          <Icon icon={labelIcon} color='inherit' />
          <Typography
            variant='body2'
            sx={{ flexGrow: 1, fontWeight: 'inherit' }}
            {...(href
              ? {
                  component: StyledLink,
                  href
                }
              : {})}
          >
            {labelText}
          </Typography>
          {labelInfo ? (
            <Typography variant='caption' color='inherit'>
              {labelInfo}
            </Typography>
          ) : null}
        </Box>
      }
    />
  )
}

const CustomTreeItem = forwardRef(function CustomTreeItem(props, ref) {
  return <StyledTreeItem ContentComponent={CustomContent} {...props} ref={ref} />
})

const CatalogTreeItem = props => {
  return <CustomTreeItem labelIcon={'bx:book'} level={0} {...props} />
}

const SchemaTreeItem = props => {
  return <CustomTreeItem labelIcon={'bx:coin-stack'} level={1} {...props} />
}

const TableTreeItem = props => {
  return <CustomTreeItem labelIcon={'bx:table'} level={2} {...props} />
}

const MetalakeTree = props => {
  const { routeParams } = props
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  const handleToggle = async (event, nodeIds) => {
    const isExpanding = nodeIds.some(nodeId => !store.expendedTreeNode.includes(nodeId))

    if (isExpanding) {
      dispatch(setIntoTreeAction({ nodeIds }))
    }

    dispatch(setExpandedTreeNode(nodeIds))
  }

  const handleSelect = (event, nodeId) => {
    event.stopPropagation()
  }

  return (
    <TreeView
      onNodeToggle={handleToggle}
      onNodeSelect={handleSelect}
      expanded={store.expendedTreeNode}
      selected={store.selectedTreeNode}
      defaultExpandIcon={
        <Box sx={{ display: 'flex' }}>
          <Icon icon='bx:chevron-right' />
        </Box>
      }
      defaultCollapseIcon={
        <Box sx={{ display: 'flex' }}>
          <Icon icon='bx:chevron-down' />
        </Box>
      }
    >
      {store.isLoadedTree ? (
        store.metalakeTree.length !== 0 ? (
          (store.metalakeTree || []).map((catalog, catalogIndex) => {
            return (
              <CatalogTreeItem key={catalogIndex} nodeId={catalog.id} labelText={catalog.name} href={catalog.path}>
                {catalog.schemas.length === 0 ? (
                  <SchemaTreeItem
                    key={`${catalog.name}-${catalogIndex}-no-schema`}
                    nodeId={`${catalog.name}-${catalogIndex}-no-schema`}
                    labelIcon=''
                    labelText={'No Schemas'}
                    disabled
                  />
                ) : (
                  (catalog.schemas || []).map((schema, schemaIndex) => {
                    return (
                      <SchemaTreeItem key={schemaIndex} nodeId={schema.id} labelText={schema.name} href={schema.path}>
                        {schema.tables.length === 0 ? (
                          <TableTreeItem
                            key={`${catalog.name}-${catalogIndex}-${schema.name}-${schemaIndex}-no-schema`}
                            nodeId={`${catalog.name}-${catalogIndex}-${schema.name}-${schemaIndex}-no-schema`}
                            labelIcon=''
                            labelText={'No Tables'}
                            disabled
                          />
                        ) : (
                          (schema.tables || []).map((table, tableIndex) => {
                            return (
                              <TableTreeItem
                                key={tableIndex}
                                nodeId={table.id}
                                labelText={table.name}
                                href={table.path}
                              />
                            )
                          })
                        )}
                      </SchemaTreeItem>
                    )
                  })
                )}
              </CatalogTreeItem>
            )
          })
        ) : (
          <Box className={`twc-text-center`}>No Data</Box>
        )
      ) : (
        <Box className={`twc-w-full twc-text-center twc-h-full`}>
          {Array.from(new Array(4)).map((item, index) => {
            return <Skeleton sx={{ mx: 'auto' }} width={'80%'} key={index} height={'2.8rem'} animation='wave' />
          })}
        </Box>
      )}
    </TreeView>
  )
}

export default MetalakeTree
