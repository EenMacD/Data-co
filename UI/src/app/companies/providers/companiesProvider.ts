import CompanySearchFilters, {
    RecursiveFilterKey,
} from "../models/search-filter";

export const appliedFilters: CompanySearchFilters = {
    limit: 20,
    offset: 0,
    includeTotal: false,
};

export function applyRecursiveFilter(
    selectedValues: string[],
    filterKey: RecursiveFilterKey,
): void {
    // The key is constrained to recursive-compatible fields only.
    appliedFilters[filterKey] = selectedValues;
}
