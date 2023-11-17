import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import axios from 'axios'

export const fetchMetalakes = createAsyncThunk('appMetalakes/fetchMetalakes', async params => {
  const response = await axios.get('/apis/metalakes', {
    params
  })

  return response.data
})

export const createMetalake = createAsyncThunk(
  'appMetalakes/createMetalake',
  async (metalake, { getState, dispatch }) => {
    const response = await axios.post('/apis/metalakes', {
      data: {
        metalake
      }
    })

    dispatch(fetchMetalakes(getState().metalake.params))

    return response.data.metalake
  }
)

export const appMetalakesSlice = createSlice({
  name: 'appMetalakes',
  initialState: {
    data: [],
    total: 1,
    params: {},
    allData: []
  },
  reducers: {},
  extraReducers: builder => {
    builder.addCase(fetchMetalakes.fulfilled, (state, action) => {
      state.data = action.payload.metalakes
      state.total = action.payload.total
      state.params = action.payload.params
      state.allData = action.payload.allData
    })
  }
})

export default appMetalakesSlice.reducer
