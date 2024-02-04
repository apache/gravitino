/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useEffect, useRef, useState } from 'react'

import Link from 'next/link'
import { useRouter } from 'next/navigation'

import { IconButton, Typography } from '@mui/material'
import { Tree } from 'antd'

import Icon from '@/components/Icon'
import clsx from 'clsx'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import {
  setExpandedNodes,
  setIntoTreeNodeWithFetch,
  removeExpandedNode,
  setSelectedNodes,
  setLoadedNodes,
  getTableDetails
} from '@/lib/store/metalakes'

import { extractPlaceholder } from '@/lib/utils'

const MetalakeTree = props => {
  const { height: offsetHeight } = props

  const router = useRouter()
  const treeRef = useRef()
  const [height, setHeight] = useState(0)
  const [isHover, setIsHover] = useState(null)

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  const handleClickIcon = (e, nodeProps) => {
    e.stopPropagation()

    if (nodeProps.data.node === 'table') {
      if (store.selectedNodes.includes(nodeProps.data.key)) {
        const pathArr = extractPlaceholder(nodeProps.data.key)
        const [metalake, catalog, schema, table] = pathArr
        dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
      }
    } else {
      dispatch(setIntoTreeNodeWithFetch({ key: nodeProps.data.key }))
    }
  }

  const onMouseEnter = (e, nodeProps) => {
    if (nodeProps.data.node === 'table') {
      if (store.selectedNodes.includes(nodeProps.data.key)) {
        setIsHover(nodeProps.data.key)
      }
    } else {
      setIsHover(nodeProps.data.key)
    }
  }

  const onMouseLeave = (e, nodeProps) => {
    setIsHover(null)
  }

  const onLoadData = node => {
    const { key, children } = node

    dispatch(setLoadedNodes([...store.loadedNodes, key]))

    return new Promise(resolve => {
      if (children && children.length !== 0) {
        resolve()

        return
      }

      dispatch(setIntoTreeNodeWithFetch({ key }))

      resolve()
    })
  }

  const onExpand = (keys, { expanded, node }) => {
    if (expanded) {
      dispatch(setExpandedNodes(keys))
    } else {
      dispatch(removeExpandedNode(node.key))
    }
  }

  const onSelect = (keys, { selected, node }) => {
    if (!selected) {
      dispatch(setSelectedNodes([node.key]))

      return
    }

    dispatch(setSelectedNodes(keys))
    router.push(node.path)
  }

  const renderIcon = nodeProps => {
    switch (nodeProps.data.node) {
      case 'catalog':
        return (
          <IconButton
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'bx:book' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )

      case 'schema':
        return (
          <IconButton
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'bx:coin-stack' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )
      case 'table':
        return (
          <IconButton
            disableRipple={!store.selectedNodes.includes(nodeProps.data.key)}
            size='small'
            sx={{ color: '#666' }}
            onClick={e => handleClickIcon(e, nodeProps)}
            onMouseEnter={e => onMouseEnter(e, nodeProps)}
            onMouseLeave={e => onMouseLeave(e, nodeProps)}
          >
            <Icon icon={isHover !== nodeProps.data.key ? 'bx:table' : 'mdi:reload'} fontSize='inherit' />
          </IconButton>
        )

      default:
        return <></>
    }
  }

  const renderNode = nodeData => {
    if (nodeData.path) {
      return <Typography sx={{ color: theme => theme.palette.text.secondary }}>{nodeData.title}</Typography>
    }

    return nodeData.title
  }

  useEffect(() => {
    if (offsetHeight) {
      setHeight(offsetHeight)
    }
  }, [offsetHeight])

  useEffect(() => {
    if (store.selectedNodes.length !== 0) {
      treeRef.current.scrollTo({ key: store.selectedNodes[0] })
    }
  }, [store.selectedNodes])

  return (
    <>
      <Tree
        ref={treeRef}
        rootStyle={{
          '& .antTreeTitle': {
            width: '100%'
          }
        }}
        treeData={store.metalakeTree}
        loadData={onLoadData}
        loadedKeys={store.loadedNodes}
        selectedKeys={store.selectedNodes}
        expandedKeys={store.expandedNodes}
        onExpand={onExpand}
        onSelect={onSelect}
        height={height}
        defaultExpandAll
        blockNode
        showIcon
        className={clsx([
          '[&_.ant-tree-switcher]:twc-inline-flex',
          '[&_.ant-tree-switcher]:twc-justify-center',
          '[&_.ant-tree-switcher]:twc-items-center',

          '[&_.ant-tree-iconEle]:twc-w-[unset]',
          '[&_.ant-tree-iconEle]:twc-inline-flex',
          '[&_.ant-tree-iconEle]:twc-items-center',

          '[&_.ant-tree-title]:twc-inline-flex',
          '[&_.ant-tree-title]:twc-w-[calc(100%-24px)]',
          '[&_.ant-tree-title]:twc-text-lg',

          '[&_.ant-tree-node-content-wrapper]:twc-inline-flex',
          '[&_.ant-tree-node-content-wrapper]:twc-items-center',
          '[&_.ant-tree-node-content-wrapper]:twc-leading-[28px]'
        ])}
        icon={nodeProps => renderIcon(nodeProps)}
        titleRender={nodeData => renderNode(nodeData)}
      />
    </>
  )
}

export default MetalakeTree
