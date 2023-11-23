/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import PageHeader from 'src/@core/components/page-header'
import Table from 'src/views/home/Table'

const Home = () => {
  return (
    <Grid container spacing={6}>
      <PageHeader
        title={<Typography sx={{ mb: 4, fontSize: '1.375rem', fontWeight: 700 }}>Metalakes</Typography>}
        subtitle={
          <Typography sx={{ color: 'text.secondary' }}>
            A metalake is the top-level container for data in Gravitino. With a metalake, Gravitino provides a 3-level
            namespace for organizing data: catalog, schemas(also called databases), and tables / view.
          </Typography>
        }
      />
      <Grid item xs={12}>
        <Table />
      </Grid>
    </Grid>
  )
}

export default Home
