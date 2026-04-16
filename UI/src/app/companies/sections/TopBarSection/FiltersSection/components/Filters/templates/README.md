# Filter Templates

Templates are reusable filter UIs. A template should only handle how the user selects values. It should not know about Redux, company search, or the API request.

The current flow is:

1. `filters.config.ts` defines the filter.
2. `pinnedFilters.ts` types the config.
3. `Filters/index.tsx` renders the right template from `filter.template`.
4. The template calls `onApply(string[])`.
5. `useCompanyFilters` saves the selected values and searches companies.

## Template Contract

Every template should accept these core props:

```ts
type TemplateProps = {
    selectedValues: string[];
    searchPlaceholder: string;
    onApply: (selectedValues: string[]) => void;
    submitOnOptionSelect?: boolean;
    onClose?: () => void;
    cardStyles?: CSSProperties;
};
```

Each template can add its own data and `getSubmitValue` type.

## What `getSubmitValue` Does

`getSubmitValue` converts the selected UI item into the value the API needs.

The user may see one value, but the API may need another. For example, SIC labels look like this:

```txt
62020 Information technology consultancy activities
```

The API only needs:

```txt
62020
```

So the industry filter uses:

```ts
function getSicCodeSubmitValue(node: RecursiveNodeModel): string {
    // SIC labels start with the code; the API expects just that code.
    const codeMatch: RegExpMatchArray | null = node.label.match(/^(\d+)\s+/);

    return codeMatch?.[1] ?? node.label;
}
```

For simple filters like status, the label is already the API value:

```ts
function getStatusSubmitValue(option: MultiSelectorOptionModel): string {
    // Status data is already the API value, so submit the option id.
    return option.id;
}
```

If a template does not receive `getSubmitValue`, it should use its default API value:

- `RecursiveSelector` submits `node.id`.
- `MultiSelector` submits `option.id`.

If a recursive filter needs the visible label instead, provide `getSubmitValue`:

```ts
function getLocationSubmitValue(node: RecursiveNodeModel): string {
    // Location API filters use the visible location name.
    return node.label;
}
```

## Adding a New Template

Use this folder shape:

```txt
templates/
  YourTemplate/
    YourTemplate.tsx
    styles.module.css
    utils.ts
    hooks/
      useYourTemplate.ts
    models/
      YourTemplateOptionModel.ts
```

Keep these responsibilities separate:

- `YourTemplate.tsx`: renders the UI and calls `onApply`.
- `useYourTemplate.ts`: manages query, selected items, and visible items.
- `utils.ts`: converts raw data, creates lookups, and maps selected values.
- `models/`: stores template-specific types.
- `styles.module.css`: stores only template styles.

## Step 1: Create The Template Type

Add a filter config type in `models/pinnedFilters.ts`.

Example:

```ts
export type YourTemplateFilter = {
    id: SomeFilterKey;
    label: string;
    text: string;
    template: "YourTemplate";
    data: YourTemplateOptionInput[];
    searchPlaceholder: string;
    getSubmitValue?: GetYourTemplateSubmitValue;
    submitOnOptionSelect?: boolean;
    cardStyles?: CSSProperties;
};
```

Then add it to the union:

```ts
export type PinnedFilter =
    | RecursiveSelectorFilter
    | MultiSelectFilter
    | YourTemplateFilter;
```

## Step 2: Render The Template

Add a case in `Filters/index.tsx`.

```tsx
case "YourTemplate":
    return (
        <YourTemplate
            data={filter.data}
            selectedValues={filters[filter.id] ?? []}
            searchPlaceholder={filter.searchPlaceholder}
            getSubmitValue={filter.getSubmitValue}
            submitOnOptionSelect={filter.submitOnOptionSelect}
            onApply={(selectedValues: string[]) =>
                applyStringArrayFilter(filter.id, selectedValues)
            }
            onClose={onClose}
            cardStyles={filter.cardStyles}
        />
    );
```

The important part is that the template receives `selectedValues` and returns `string[]` through `onApply`.

## Step 3: Attach The Template To A Filter

Add a config entry in `filters.config.ts`.

Example using `MultiSelector`:

```ts
{
    id: "status",
    label: "Status",
    text: "Select Status",
    template: "MultiSelector",
    data: statusData,
    searchPlaceholder: "Search company status",
    getSubmitValue: getStatusSubmitValue,
    submitOnOptionSelect: true,
    cardStyles: { width: "40rem" },
}
```

The `id` must exist on `CompanySearchFilters`, and that field must be a `string[]` if the template returns `string[]`.

## Step 4: Add The Filter Key

If this is a new API filter, add it to `CompanySearchFilters`.

```ts
export default interface CompanySearchFilters {
    status?: string[];
}
```

For templates that return `string[]`, use `applyStringArrayFilter` from `useCompanyFilters`.

## Checklist

Before finishing a new template:

1. The template does not import Redux or company search actions.
2. The template accepts `selectedValues` and calls `onApply(string[])`.
3. `getSubmitValue` converts UI data into API values.
4. The filter has a type in `pinnedFilters.ts`.
5. `Filters/index.tsx` has a `switch` case for the template.
6. `filters.config.ts` has a filter entry using the template.
7. The filter `id` exists in `CompanySearchFilters`.
8. `npm.cmd run lint` and `npm.cmd run build` pass.
