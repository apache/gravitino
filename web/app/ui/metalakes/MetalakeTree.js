/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useEffect, useRef, useState } from 'react'

import Link from 'next/link'

import { IconButton, styled } from '@mui/material'
import { Tree } from 'antd'

import Icon from '@/components/Icon'
import clsx from 'clsx'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { setExpandedNodes, setIntoTreeNodeWithFetch, removeExpandedNode } from '@/lib/store/metalakes'

const StyledLink = styled(Link)(({ theme }) => ({
  textDecoration: 'none',
  color: theme.palette.text.secondary,
  '&:hover': {
    color: theme.palette.text.secondary
  }
}))

const MetalakeTree = props => {
  const { height: offsetHeight } = props

  const treeRef = useRef()
  const [height, setHeight] = useState(0)
  const [loadedKeys, setLoadedKeys] = useState([])
  const [selectedKeys, setSelectedKeys] = useState([])

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  const handleClickIcon = () => {}

  const onLoadData = treeNode => {
    const { key, children } = treeNode

    setLoadedKeys([...loadedKeys, key])

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

  const renderIcon = nodeProps => {
    switch (nodeProps.data.node) {
      case 'catalog':
        return (
          <IconButton size='small' sx={{ color: '#666' }} onClick={() => handleClickIcon()}>
            <Icon icon={'bx:book'} fontSize='inherit' />
          </IconButton>
        )

      case 'schema':
        return (
          <IconButton size='small' sx={{ color: '#666' }} onClick={() => handleClickIcon()}>
            <Icon icon={'bx:coin-stack'} fontSize='inherit' />
          </IconButton>
        )
      case 'table':
        return (
          <IconButton size='small' sx={{ color: '#666' }} onClick={() => handleClickIcon()}>
            <Icon icon={'bx:table'} fontSize='inherit' />
          </IconButton>
        )

      default:
        return <></>
    }
  }

  const renderNode = nodeData => {
    if (nodeData.path) {
      return (
        <StyledLink className='twc-w-full twc-text-base twc-pl-1' href={nodeData.path}>
          {nodeData.title}
        </StyledLink>
      )
    }

    return nodeData.title
  }

  useEffect(() => {
    if (offsetHeight) {
      setHeight(offsetHeight)
    }
  }, [offsetHeight])

  useEffect(() => {
    if (store.selectedTreeNode) {
      setSelectedKeys([store.selectedTreeNode])
      treeRef.current.scrollTo({ key: store.selectedTreeNode })
    }
  }, [store.selectedTreeNode])

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
        loadData={node => onLoadData(node)}
        loadedKeys={loadedKeys}
        selectedKeys={selectedKeys}
        expandedKeys={store.expandedNodes}
        onExpand={onExpand}
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
