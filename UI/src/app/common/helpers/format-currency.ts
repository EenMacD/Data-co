// Helper function to format currency
export function formatCurrency(
    value: { Float64: number; Valid: boolean } | null,
): string {
    if (!value || !value.Valid || value.Float64 === 0) {
        return "N/A";
    }
    return `£${(value.Float64 / 1000000).toFixed(1)}M`;
}
