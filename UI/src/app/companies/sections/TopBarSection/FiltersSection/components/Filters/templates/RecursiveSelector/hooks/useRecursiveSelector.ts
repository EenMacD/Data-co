import { useState, type ChangeEvent } from "react";
import type { RecursiveNode } from "../models/recursiveNode";
import type { LocalityTree } from "../utils";
import { filterRecursiveNodes, toRecursiveNodes } from "../utils";

type UseRecursiveSelectorResult = {
    nodes: RecursiveNode[];
    query: string;
    visibleNodes: RecursiveNode[];
    selectedOptions: string[];
    handleQueryChange: (event: ChangeEvent<HTMLInputElement>) => void;
    toggleOption: (id: string) => void;
};

export function useRecursiveSelector(
    localityTree: LocalityTree,
): UseRecursiveSelectorResult {
    const [query, setQuery] = useState("");
    const [selectedOptions, setSelectedOptions] = useState<string[]>([]);

    // Build the full tree once per render, then derive the visible tree from it.
    const nodes: RecursiveNode[] = toRecursiveNodes(localityTree);
    const visibleNodes: RecursiveNode[] = filterRecursiveNodes(nodes, query);

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
