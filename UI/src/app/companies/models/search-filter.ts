export default interface CompanySearchFilters {
    // searchTerm?: string;
    industry?: string[];
    location?: string[];
    status?: string[];
    // revenue?: string;
    // employees?: string;
    // profitability?: string;
    // companySize?: string;
    // netAssets?: string;
    // debtLevel?: string;
    limit?: number;
    offset?: number;
    orderBy?: string;
    includeTotal?: boolean;
}

export type RecursiveFilterKey = {
    // Keep only filter keys whose values can store recursive selector output.
    [Key in keyof CompanySearchFilters]-?: CompanySearchFilters[Key] extends
        | string[]
        | undefined
        ? Key
        : never;
}[keyof CompanySearchFilters];

export type MultiSelectorFilterKey = {
    // Keep only filter keys whose values can store Multi Selector value.
    [Key in keyof CompanySearchFilters]-?: CompanySearchFilters[Key] extends
        | string[]
        | undefined
        ? Key
        : never;
}[keyof CompanySearchFilters];

export type StringArrayFilterKey = RecursiveFilterKey | MultiSelectorFilterKey;
