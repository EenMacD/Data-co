// Helper function to format currency
export function formatCurrency(
    value: { Float64: number; Valid: boolean } | null,
): string {
    if (!value || !value.Valid || value.Float64 === 0) {
        return "N/A";
    }
    return `£${(value.Float64 / 1000000).toFixed(1)}M`;
}

// Helper function to get string value from nullable field
export function getString(
    value: { String: string; Valid: boolean } | null,
    defaultValue: string = "N/A",
): string {
    if (
        !value ||
        !value.Valid ||
        value.String === "NaN" ||
        value.String === ""
    ) {
        return defaultValue;
    }
    return value.String;
}
