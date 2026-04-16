import { useCallback } from "react";
import type { StringArrayFilterKey } from "@/app/companies/models/search-filter";
import {
    fetchSearchCompanies,
    setStringArrayFilter,
} from "@/app/companies/store/features/searchCompaniesSlice";
import { useAppDispatch, useAppSelector } from "@/app/store/hooks";

export function useCompanyFilters() {
    const dispatch = useAppDispatch();
    const filters = useAppSelector((state) => state.companiesSearch.filters);

    const applyStringArrayFilter = useCallback(
        (filterKey: StringArrayFilterKey, selectedValues: string[]): void => {
            dispatch(setStringArrayFilter({ filterKey, selectedValues }));
            void dispatch(fetchSearchCompanies());
        },
        [dispatch],
    );

    return {
        filters,
        applyStringArrayFilter,
    };
}
