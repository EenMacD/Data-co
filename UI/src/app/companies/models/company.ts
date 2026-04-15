export default interface Company {
    company_number: string;
    company_name: string;
    company_status: string;
    locality: { String: string; Valid: boolean } | null;
    region: { String: string; Valid: boolean } | null;
    postal_code: { String: string; Valid: boolean } | null;
    // sic_code: { String: string; Valid: boolean } | null;
    // industry_category: { String: string; Valid: boolean } | null;
    // incorporation_date: string | null;
    // turnover: { Float64: number; Valid: boolean } | null;
    // profit_after_tax: { Float64: number; Valid: boolean } | null;
    // total_assets: { Float64: number; Valid: boolean } | null;
    // net_worth: { Float64: number; Valid: boolean } | null;
    // profit_margin: { Float64: number; Valid: boolean } | null;
    // latest_accounts_date: string | null;
    // active_officers_count: number;
}
