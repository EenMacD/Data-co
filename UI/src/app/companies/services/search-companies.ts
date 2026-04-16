import type CompanySearchFilters from "../models/search-filter";
import { SearchResponse } from "../models/search-response";

export async function searchCompanies(
    filters: CompanySearchFilters,
): Promise<SearchResponse> {
    const apiRoute = "http://localhost:8080/api";

    console.log(JSON.stringify(filters));

    try {
        const response: Response = await fetch(`${apiRoute}/companies/search`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(filters),
        });

        if (!response.ok) {
            return {
                companies: [],
                total: 0,
                limit: filters.limit ?? 0,
                offset: filters.offset ?? 0,
                has_more: false,
                error: `Unable to load companies right now (${response.status}).`,
            };
        }

        return response.json();
    } catch {
        return {
            companies: [],
            total: 0,
            limit: filters.limit ?? 0,
            offset: filters.offset ?? 0,
            has_more: false,
            error: "The companies API is offline right now.",
        };
    }
}
