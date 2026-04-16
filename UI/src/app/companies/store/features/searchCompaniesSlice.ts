import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { RootState } from "@/app/store/store";
import type CompanySearchFilters from "../../models/search-filter";
import type { StringArrayFilterKey } from "../../models/search-filter";
import type { SearchResponse } from "../../models/search-response";
import { searchCompanies } from "../../services/search-companies";

export interface SearchCompaniesState {
    filters: CompanySearchFilters;
    companies: SearchResponse["companies"];
    total: number;
    has_more: boolean;
    error?: string;
    status: "idle" | "loading" | "succeeded" | "failed";
}

const initialState: SearchCompaniesState = {
    filters: {
        limit: 20,
        offset: 0,
        includeTotal: true,
    },
    companies: [],
    total: 0,
    has_more: false,
    error: undefined,
    status: "idle",
};

export const fetchSearchCompanies = createAsyncThunk<
    SearchResponse,
    void,
    { state: RootState }
>(
    "searchCompanies/fetchSearchCompanies",
    async (_arg, { getState }) => {
        const { filters } = getState().companiesSearch;
        return searchCompanies(filters);
    },
);

const searchCompaniesSlice = createSlice({
    name: "searchCompanies",
    initialState,
    reducers: {
        setOffset: (state, action: PayloadAction<number>) => {
            state.filters.offset = action.payload;
        },
        setLimit: (state, action: PayloadAction<number>) => {
            state.filters.limit = action.payload;
        },
        setStringArrayFilter: (
            state,
            action: PayloadAction<{
                filterKey: StringArrayFilterKey;
                selectedValues: string[];
            }>,
        ) => {
            state.filters[action.payload.filterKey] = action.payload.selectedValues;
            state.filters.offset = 0;
        },
    },
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
                state.has_more = action.payload.has_more ?? false;
                state.error = action.payload.error;
            })
            .addCase(fetchSearchCompanies.rejected, (state) => {
                state.status = "failed";
                state.companies = [];
                state.total = 0;
                state.has_more = false;
                state.error = "Unable to load companies right now.";
            });
    },
});

export const { setOffset, setLimit, setStringArrayFilter } =
    searchCompaniesSlice.actions;

export default searchCompaniesSlice.reducer;
