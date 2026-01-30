const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL;

export interface CompanySearchFilters {
    searchTerm?: string;
    industry?: string;
    location?: string;
    revenue?: string;
    employees?: string;
    profitability?: string;
    companySize?: string;
    companyStatus?: string;
    netAssets?: string;
    debtLevel?: string;
    limit?: number;
    offset?: number;
}

export interface Company {
    id: number;
    company_number: string;
    company_name: string;
    company_status: string;
    locality: { String: string; Valid: boolean } | null;
    region: { String: string; Valid: boolean } | null;
    postal_code: { String: string; Valid: boolean } | null;
    primary_sic_code: { String: string; Valid: boolean } | null;
    industry_category: { String: string; Valid: boolean } | null;
    incorporation_date: string | null;
    turnover: { Float64: number; Valid: boolean } | null;
    profit_after_tax: { Float64: number; Valid: boolean } | null;
    total_assets: { Float64: number; Valid: boolean } | null;
    net_worth: { Float64: number; Valid: boolean } | null;
    profit_margin: { Float64: number; Valid: boolean } | null;
    latest_accounts_date: string | null;
    active_officers_count: number;
}

export interface SearchResponse {
    companies: Company[];
    total: number;
    limit: number;
    offset: number;
    has_more: boolean;
}

export interface CountResponse {
    total: number;
}

export async function searchCompanies(filters: CompanySearchFilters): Promise<SearchResponse> {
    const response = await fetch(`${API_BASE_URL}/companies/search`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(filters),
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}

export async function countCompanies(filters: CompanySearchFilters): Promise<CountResponse> {
    const response = await fetch(`${API_BASE_URL}/companies/count`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(filters),
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}

export async function getCompany(id: number): Promise<Company> {
    const response = await fetch(`${API_BASE_URL}/companies/${id}`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        },
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }

    return response.json();
}

// Helper function to format currency
export function formatCurrency(value: { Float64: number; Valid: boolean } | null): string {
    if (!value || !value.Valid || value.Float64 === 0) {
        return 'N/A';
    }
    return `£${(value.Float64 / 1000000).toFixed(1)}M`;
}

// Helper function to get string value from nullable field
export function getString(value: { String: string; Valid: boolean } | null, defaultValue: string = 'N/A'): string {
    if (!value || !value.Valid || value.String === 'NaN' || value.String === '') {
        return defaultValue;
    }
    return value.String;
}
