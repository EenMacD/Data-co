import Company from "./company";

export interface SearchResponse {
    companies?: Company[];
    total?: number;
    limit?: number;
    offset?: number;
    has_more?: boolean;
    error?: string;
}
