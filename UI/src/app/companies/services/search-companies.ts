import { appliedFilters } from "../providers/companiesProvider";
import { SearchResponse } from "../models/search-response";

export async function searchCompanies(): Promise<SearchResponse> {
    const apiRoute: string | undefined =
        process.env.API_URL ?? process.env.DOCKER_API_URL;

    if (!apiRoute) {
        return {
            companies: [],
            total: 0,
            limit: appliedFilters.limit ?? 0,
            offset: appliedFilters.offset ?? 0,
            has_more: false,
            error: "API URL is not configured.",
        };
    }

    console.log(apiRoute);

    try {
        const response: Response = await fetch(`${apiRoute}/companies/search`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(appliedFilters),
        });

        if (!response.ok) {
            return {
                companies: [],
                total: 0,
                limit: appliedFilters.limit ?? 0,
                offset: appliedFilters.offset ?? 0,
                has_more: false,
                error: `Unable to load companies right now (${response.status}).`,
            };
        }

        return response.json();
    } catch {
        return {
            companies: [],
            total: 0,
            limit: appliedFilters.limit ?? 0,
            offset: appliedFilters.offset ?? 0,
            has_more: false,
            error: "The companies API is offline right now.",
        };
    }
}
