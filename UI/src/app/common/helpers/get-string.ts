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
