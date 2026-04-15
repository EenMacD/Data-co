import type { RecursiveFilterKey } from "@/app/companies/models/search-filter";

export type SelectedNodesRequestModel = {
    selectedValues: string[];
    filterId: RecursiveFilterKey;
};
