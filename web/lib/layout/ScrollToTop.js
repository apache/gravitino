/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Zoom, useScrollTrigger } from '@mui/material'

const ScrollToTop = props => {
  const { children, className } = props

  const trigger = useScrollTrigger({
    threshold: 400,
    disableHysteresis: true
  })

  const handleClick = () => {
    const anchor = document.querySelector('body')
    if (anchor) {
      anchor.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <Zoom in={trigger}>
      <div
        className={`${className} twc-z-[12] twc-fixed twc-right-6 twc-bottom-10`}
        onClick={handleClick}
        role='presentation'
      >
        {children}
      </div>
    </Zoom>
  )
}

export default ScrollToTop
