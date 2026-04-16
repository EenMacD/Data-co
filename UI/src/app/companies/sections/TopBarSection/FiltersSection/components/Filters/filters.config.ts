import type { PinnedFilter } from "./models/pinnedFilters";
import type { MultiSelectorOptionModel } from "./templates/MultiSelector/models/MultiSelectorOptionModel";
import type { RecursiveNodeModel } from "./templates/RecursiveSelector/models/RecursiveNodeModel";
import localityData from "./data/locality.json";
import sicData from "./data/sic.json";
import statusData from "./data/company_status.json";

function getSicCodeSubmitValue(node: RecursiveNodeModel): string {
    // SIC labels start with the code; the API expects just that code.
    const codeMatch: RegExpMatchArray | null = node.label.match(/^(\d+)\s+/);

    return codeMatch?.[1] ?? node.label;
}

function getLocationSubmitValue(node: RecursiveNodeModel): string {
    // Location API filters use the visible location name.
    return node.label;
}

function getStatusSubmitValue(option: MultiSelectorOptionModel): string {
    // Status data is already the API value, so submit the option id.
    return option.id;
}

const pinnedFilters: PinnedFilter[] = [
    {
        id: "location",
        label: "Location",
        text: "Select Location",
        template: "Recursive",
        data: localityData,
        searchPlaceholder: "Search Country, City or Town",
        getSubmitValue: getLocationSubmitValue,
        submitOnOptionSelect: true,
    },
    {
        id: "industry",
        label: "Industry",
        text: "Select industry",
        template: "Recursive",
        data: sicData,
        searchPlaceholder: "Search industry or SIC code",
        getSubmitValue: getSicCodeSubmitValue,
        disableFirstNodeSelection: true,
        submitOnOptionSelect: false,
        cardStyles: { width: "40rem" },
    },
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
    },
];

export default pinnedFilters;
