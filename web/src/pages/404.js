/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Link from 'next/link'

import Button from '@mui/material/Button'
import Typography from '@mui/material/Typography'
import Box from '@mui/material/Box'
import { styled } from '@mui/material/styles'

import BlankLayout from 'src/@core/layouts/BlankLayout'

const BoxWrapper = styled(Box)(({ theme }) => ({
  [theme.breakpoints.down('md')]: {
    width: '90vw'
  }
}))

const Error404 = () => {
  return (
    <Box className='content-center'>
      <Box sx={{ p: 5, display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
        <BoxWrapper>
          <Typography variant='h4' sx={{ mb: 2 }}>
            Page Not Found
          </Typography>
          <Button href='/' component={Link} variant='contained'>
            Back to Home
          </Button>
        </BoxWrapper>
      </Box>
    </Box>
  )
}
Error404.getLayout = page => <BlankLayout>{page}</BlankLayout>

export default Error404
