import { CompanySearchFilters } from "../models/companies-search-filters";
import { Company } from "../models/company";
import { CountResponse } from "../models/count-response";
import { SearchResponse } from "../models/search-response";

const API_BASE_URL = process.env.API_URL;

// fetch companies based on search filters
export async function searchCompanies(
    filters: CompanySearchFilters,
): Promise<SearchResponse> {
    console.log(API_BASE_URL);

    const response = await fetch(`${API_BASE_URL}/companies/search`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(filters),
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}

// get a single company by id
export async function getCompany(id: number): Promise<Company> {
    const response = await fetch(`${API_BASE_URL}/companies/${id}`, {
        method: "GET",
        headers: {
            "Content-Type": "application/json",
        },
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}

// count companies based on search filters
export async function countCompanies(
    filters: CompanySearchFilters,
): Promise<CountResponse> {
    const response = await fetch(`${API_BASE_URL}/companies/count`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(filters),
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}
