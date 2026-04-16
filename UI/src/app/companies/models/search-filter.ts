export default interface CompanySearchFilters {
    // searchTerm?: string;
    industry?: string[];
    locations?: string[];
    // revenue?: string;
    // employees?: string;
    // profitability?: string;
    // companySize?: string;
    // companyStatus?: string;
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
