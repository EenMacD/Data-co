import type {
    MultiSelectorFilterKey,
    RecursiveFilterKey as RecursiveSelectorFilterKey,
} from "@/app/companies/models/search-filter";
import type { CSSProperties, ReactElement } from "react";
import type { GetRecursiveNodeSubmitValue } from "../templates/RecursiveSelector/utils";
import type { RecursiveTree } from "../templates/RecursiveSelector/models/RecursiveNodeModel";
import type { MultiSelectorOptionInput } from "../templates/MultiSelector/models/MultiSelectorOptionModel";
import type { GetMultiSelectorSubmitValue } from "../templates/MultiSelector/utils";

export type FilterTemplateProps = {
    onClose: () => void;
};

export type RecursiveSelectorFilter = {
    id: RecursiveSelectorFilterKey;
    label: string;
    text: string;
    template: "Recursive";
    data: RecursiveTree;
    searchPlaceholder: string;
    getSubmitValue?: GetRecursiveNodeSubmitValue;
    disableFirstNodeSelection?: boolean;
    submitOnOptionSelect?: boolean;
    cardStyles?: CSSProperties;
};

export type MultiSelectFilter = {
    id: MultiSelectorFilterKey;
    label: string;
    text: string;
    template: "MultiSelector";
    data: MultiSelectorOptionInput[];
    searchPlaceholder: string;
    getSubmitValue?: GetMultiSelectorSubmitValue;
    submitOnOptionSelect?: boolean;
    cardStyles?: CSSProperties;
};

export type FilterTemplateRenderer = (
    props: FilterTemplateProps,
) => ReactElement;

export type PinnedFilter = RecursiveSelectorFilter | MultiSelectFilter;
