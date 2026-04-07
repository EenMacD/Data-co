import CompanySearchFilters from "../models/search-filter";
import { SearchResponse } from "../models/search-response";

export async function searchCompanies(
    filters: CompanySearchFilters,
): Promise<SearchResponse> {
    const apiRoute = process.env.API_URL;

    const response = await fetch(`${apiRoute}/companies/search`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(filters),
    });

    console.log(JSON.stringify(filters));

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}
