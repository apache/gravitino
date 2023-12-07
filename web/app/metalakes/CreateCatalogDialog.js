/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useState, forwardRef, useEffect, Fragment } from 'react'

import {
  Box,
  Grid,
  Button,
  Dialog,
  TextField,
  Typography,
  DialogContent,
  DialogActions,
  IconButton,
  Fade,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  FormHelperText
} from '@mui/material'

import Icon from '@/components/Icon'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { createCatalog } from '@/lib/store/metalakes'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { providers } from '@/lib/utils/initial'

const defaultValues = {
  name: '',
  type: 'relational',
  provider: 'hive',
  comment: '',
  propItems: providers[0].defaultProps
}

const providerTypeValues = providers.map(i => i.value)

const schema = yup.object().shape({
  name: yup.string().required(),
  type: yup.mixed().oneOf(['relational']).required(),
  provider: yup.mixed().oneOf(providerTypeValues).required(),
  propItems: yup.array().of(
    yup.object().shape({
      required: yup.boolean(),
      key: yup.string().required(),
      value: yup.string().when('required', {
        is: true,
        then: schema => schema.required()
      })
    })
  )
})

const Transition = forwardRef(function Transition(props, ref) {
  return <Fade ref={ref} {...props} />
})

const CreateCatalogDialog = props => {
  const { open, setOpen, type = 'create', data = {}, routeParams } = props
  const { metalake } = routeParams

  const dispatch = useAppDispatch()

  const typeText = type === 'create' ? 'Create' : 'Update'

  const [innerProps, setInnerProps] = useState(providers[0].defaultProps)

  const [cacheData, setCacheData] = useState()

  const {
    control,
    reset,
    watch,
    setValue,
    handleSubmit,
    formState: { errors }
  } = useForm({
    defaultValues,
    mode: 'onChange',
    resolver: yupResolver(schema)
  })

  const providerSelect = watch('provider')

  const handleFormChange = ({ index, event }) => {
    let data = [...innerProps]
    data[index][event.target.name] = event.target.value
    setInnerProps(data)
    setValue('propItems', data)
  }

  const addFields = () => {
    let newField = { key: '', value: '', required: false }

    setInnerProps([...innerProps, newField])
    setValue('propItems', [...innerProps, newField])
  }

  const removeFields = index => {
    let data = [...innerProps]
    data.splice(index, 1)
    setInnerProps(data)
    setValue('propItems', data)
  }

  const hideField = field => {
    if (!field) {
      return true
    }
    const parentField = innerProps.find(i => i.key === 'catalog-backend')
    const check = parentField && parentField.value === field.hide

    return check
  }

  const handleClose = () => {
    reset()
    setInnerProps(providers[0].defaultProps)
    setValue('propItems', providers[0].defaultProps)
    setOpen(false)
  }

  const onSubmit = data => {
    const { propItems, ...mainData } = data

    const properties = innerProps.reduce((acc, item) => {
      acc[item.key] = item.value

      return acc
    }, {})

    const catalogData = {
      ...mainData,
      properties
    }

    if (type === 'create') {
      dispatch(createCatalog({ data: catalogData, metalake }))
    }

    handleClose()
  }

  useEffect(() => {
    const providerItemIndex = providers.findIndex(i => i.value === providerSelect)
    setInnerProps(providers[providerItemIndex].defaultProps)
  }, [providerSelect, setInnerProps])

  useEffect(() => {
    if (open && JSON.stringify(data) !== '{}') {
      setCacheData(data)
      const { properties } = data

      const propsArr = Object.keys(properties).map(item => {
        return {
          key: item,
          value: properties[item]
        }
      })

      setInnerProps(propsArr)

      setValue('name', data.name)
      setValue('comment', data.comment)
    }
  }, [open, data, setValue])

  return (
    <Dialog fullWidth maxWidth='sm' scroll='body' TransitionComponent={Transition} open={open} onClose={handleClose}>
      <form onSubmit={handleSubmit(onSubmit)}>
        <DialogContent
          sx={{
            position: 'relative',
            pb: theme => `${theme.spacing(8)} !important`,
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <IconButton
            size='small'
            onClick={() => setOpen(false)}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {typeText} Catalog
            </Typography>
          </Box>

          <Grid container spacing={6}>
            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='name'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Name'
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.name)}
                    />
                  )}
                />
                {errors.name && <FormHelperText sx={{ color: 'error.main' }}>{errors.name.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <InputLabel id='select-catalog-type' error={Boolean(errors.type)}>
                  Type
                </InputLabel>
                <Controller
                  name='type'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <Select
                      value={value}
                      label='Type'
                      defaultValue='relational'
                      onChange={onChange}
                      error={Boolean(errors.type)}
                      labelId='select-catalog-type'
                    >
                      <MenuItem value={'relational'}>relational</MenuItem>
                    </Select>
                  )}
                />
                {errors.type && <FormHelperText sx={{ color: 'error.main' }}>{errors.type.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <InputLabel id='select-catalog-provider' error={Boolean(errors.provider)}>
                  Provider
                </InputLabel>
                <Controller
                  name='provider'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <Select
                      value={value}
                      label='Provider'
                      defaultValue='hive'
                      onChange={onChange}
                      error={Boolean(errors.provider)}
                      labelId='select-catalog-provider'
                    >
                      <MenuItem value={'hive'}>hive</MenuItem>
                      <MenuItem value={'lakehouse-iceberg'}>iceberg</MenuItem>
                      <MenuItem value={'jdbc-mysql'}>mysql</MenuItem>
                      <MenuItem value={'jdbc-postgresql'}>postgresql</MenuItem>
                    </Select>
                  )}
                />
                {errors.provider && (
                  <FormHelperText sx={{ color: 'error.main' }}>{errors.provider.message}</FormHelperText>
                )}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='comment'
                  control={control}
                  rules={{ required: false }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Comment'
                      multiline
                      rows={2}
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.comment)}
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  !hideField(item) && (
                    <Fragment key={index}>
                      <Grid item xs={12} sx={{ '& + &': { mt: 2 } }}>
                        <FormControl fullWidth>
                          <Box>
                            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                              <Box>
                                <TextField
                                  size='small'
                                  name='key'
                                  label='Key'
                                  value={item.key}
                                  disabled={item.required}
                                  onChange={event => handleFormChange({ index, event })}
                                />
                              </Box>
                              <Box>
                                {item.select ? (
                                  <Select
                                    name='value'
                                    value={item.value}
                                    size='small'
                                    sx={{ width: 195 }}
                                    onChange={event => handleFormChange({ index, event })}
                                  >
                                    {item.select.map(selectItem => (
                                      <MenuItem key={selectItem} value={selectItem}>
                                        {selectItem}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                ) : (
                                  <TextField
                                    size='small'
                                    name='value'
                                    label='Value'
                                    error={item.required && item.value === ''}
                                    value={item.value}
                                    onChange={event => handleFormChange({ index, event })}
                                  />
                                )}
                              </Box>

                              {!item.required ? (
                                <Box sx={{ minWidth: 40 }}>
                                  <IconButton onClick={() => removeFields(index)}>
                                    <Icon icon='mdi:minus-circle-outline' />
                                  </IconButton>
                                </Box>
                              ) : (
                                <Box sx={{ minWidth: 40 }}></Box>
                              )}
                            </Box>
                          </Box>
                          <FormHelperText
                            sx={{
                              color: item.required && item.value === '' ? 'error.main' : 'text.main',
                              maxWidth: 'calc(100% - 40px)'
                            }}
                          >
                            {item.description}
                          </FormHelperText>
                        </FormControl>
                      </Grid>
                    </Fragment>
                  )
                )
              })}
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addFields}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
              >
                Add Property
              </Button>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions
          sx={{
            justifyContent: 'center',
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pb: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <Button variant='contained' sx={{ mr: 1 }} type='submit'>
            {typeText}
          </Button>
          <Button variant='outlined' onClick={handleClose}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default CreateCatalogDialog
