import { useEffect, useMemo, useState, type ChangeEvent } from "react";
import type {
    RecursiveNodeModel,
    RecursiveTree,
} from "../models/RecursiveNodeModel";
import type { GetRecursiveNodeSubmitValue } from "../utils";
import {
    filterRecursiveNodes,
    getNodeIdsForSubmitValues,
    toRecursiveNodes,
} from "../utils";

type UseRecursiveSelectorResult = {
    nodes: RecursiveNodeModel[];
    query: string;
    visibleNodes: RecursiveNodeModel[];
    selectedOptions: string[];
    handleQueryChange: (event: ChangeEvent<HTMLInputElement>) => void;
    toggleOption: (id: string) => void;
};

type UseRecursiveSelectorParams = {
    tree: RecursiveTree;
    selectedValues: string[];
    getSubmitValue: GetRecursiveNodeSubmitValue;
};

export function useRecursiveSelector({
    tree,
    selectedValues,
    getSubmitValue,
}: UseRecursiveSelectorParams): UseRecursiveSelectorResult {
    const nodes: RecursiveNodeModel[] = useMemo(
        () => toRecursiveNodes(tree),
        [tree],
    );
    const appliedOptionIds: string[] = useMemo(
        () => getNodeIdsForSubmitValues(nodes, selectedValues, getSubmitValue),
        [getSubmitValue, nodes, selectedValues],
    );
    const [query, setQuery] = useState("");
    const [selectedOptions, setSelectedOptions] =
        useState<string[]>(appliedOptionIds);

    useEffect(() => {
        setSelectedOptions(appliedOptionIds);
    }, [appliedOptionIds]);

    const visibleNodes: RecursiveNodeModel[] = useMemo(
        () => filterRecursiveNodes(nodes, query),
        [nodes, query],
    );

    function handleQueryChange(event: ChangeEvent<HTMLInputElement>): void {
        setQuery(event.target.value);
    }

    function toggleOption(id: string): void {
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
