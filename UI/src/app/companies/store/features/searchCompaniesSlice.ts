import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import type { SearchResponse } from "../../models/search-response";
import { searchCompanies } from "../../services/search-companies";

export interface SearchCompaniesState extends SearchResponse {
    status: "idle" | "loading" | "succeeded" | "failed";
}

const initialState: SearchCompaniesState = {
    companies: [],
    total: 0,
    limit: 0,
    offset: 0,
    has_more: false,
    error: undefined,
    status: "idle",
};

export const fetchSearchCompanies = createAsyncThunk<SearchResponse>(
    "searchCompanies/fetchSearchCompanies",
    searchCompanies,
);

const searchCompaniesSlice = createSlice({
    name: "searchCompanies",
    initialState,
    reducers: {},
    extraReducers: (builder) => {
        builder
            .addCase(fetchSearchCompanies.pending, (state) => {
                state.status = "loading";
                state.error = undefined;
            })
            .addCase(fetchSearchCompanies.fulfilled, (state, action) => {
                state.status = "succeeded";
                state.companies = action.payload.companies ?? [];
                state.total = action.payload.total ?? 0;
                state.limit = action.payload.limit ?? 0;
                state.offset = action.payload.offset ?? 0;
                state.has_more = action.payload.has_more ?? false;
                state.error = action.payload.error;
            })
            .addCase(fetchSearchCompanies.rejected, (state) => {
                state.status = "failed";
                state.companies = [];
                state.total = 0;
                state.limit = 0;
                state.offset = 0;
                state.has_more = false;
                state.error = "Unable to load companies right now.";
            });
    },
});

export default searchCompaniesSlice.reducer;
