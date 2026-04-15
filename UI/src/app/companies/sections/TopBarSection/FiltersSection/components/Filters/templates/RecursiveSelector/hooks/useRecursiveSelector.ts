import { useEffect, useState, type ChangeEvent } from "react";
import type { RecursiveNodeModel } from "../models/RecursiveNodeModel";
import type { LocalityTree } from "../utils";
import { filterRecursiveNodes, toRecursiveNodes } from "../utils";
import type { RecursiveFilterKey } from "@/app/companies/models/search-filter";
import { useAppSelector } from "@/app/store/hooks";

type UseRecursiveSelectorResult = {
    nodes: RecursiveNodeModel[];
    query: string;
    visibleNodes: RecursiveNodeModel[];
    selectedOptions: string[];
    handleQueryChange: (event: ChangeEvent<HTMLInputElement>) => void;
    toggleOption: (id: string) => void;
};

export function useRecursiveSelector(
    localityTree: LocalityTree,
    filterId: RecursiveFilterKey,
): UseRecursiveSelectorResult {
    const appliedOptions: string[] = useAppSelector((state) => {
        return state.companiesSearch.filters[filterId] ?? [];
    });
    const [query, setQuery] = useState("");
    const [selectedOptions, setSelectedOptions] =
        useState<string[]>(appliedOptions);

    useEffect(() => {
        setSelectedOptions(appliedOptions);
    }, [appliedOptions]);

    // Build the full tree once per render, then derive the visible tree from it.
    const nodes: RecursiveNodeModel[] = toRecursiveNodes(localityTree);
    const visibleNodes: RecursiveNodeModel[] = filterRecursiveNodes(
        nodes,
        query,
    );

    function handleQueryChange(event: ChangeEvent<HTMLInputElement>): void {
        setQuery(event.target.value);
    }

    function toggleOption(id: string): void {
        // Toggle the selected id in place.
        setSelectedOptions((prev: string[]) =>
            prev.includes(id)
                ? prev.filter((optionId: string) => optionId !== id)
                : [...prev, id],
        );
    }

    return {
        nodes,
        query,
        visibleNodes,
        selectedOptions,
        handleQueryChange,
        toggleOption,
    };
}
