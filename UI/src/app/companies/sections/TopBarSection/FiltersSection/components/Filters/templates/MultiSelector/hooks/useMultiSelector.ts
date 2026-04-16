import { useEffect, useMemo, useState, type ChangeEvent } from "react";
import type {
    MultiSelectorOptionInput,
    MultiSelectorOptionModel,
} from "../models/MultiSelectorOptionModel";
import type { GetMultiSelectorSubmitValue } from "../utils";
import {
    getOptionIdsForSubmitValues,
    toMultiSelectorOptions,
} from "../utils";

type UseMultiSelectorParams = {
    data: MultiSelectorOptionInput[];
    selectedValues: string[];
    getSubmitValue: GetMultiSelectorSubmitValue;
};

type UseMultiSelectorResult = {
    options: MultiSelectorOptionModel[];
    query: string;
    visibleOptions: MultiSelectorOptionModel[];
    selectedOptionIds: string[];
    handleQueryChange: (event: ChangeEvent<HTMLInputElement>) => void;
    toggleOption: (id: string) => void;
};

export function useMultiSelector({
    data,
    selectedValues,
    getSubmitValue,
}: UseMultiSelectorParams): UseMultiSelectorResult {
    const options: MultiSelectorOptionModel[] = useMemo(
        () => toMultiSelectorOptions(data),
        [data],
    );
    const appliedOptionIds: string[] = useMemo(
        () => getOptionIdsForSubmitValues(options, selectedValues, getSubmitValue),
        [getSubmitValue, options, selectedValues],
    );
    const [query, setQuery] = useState("");
    const [selectedOptionIds, setSelectedOptionIds] =
        useState<string[]>(appliedOptionIds);

    useEffect(() => {
        setSelectedOptionIds(appliedOptionIds);
    }, [appliedOptionIds]);

    const visibleOptions: MultiSelectorOptionModel[] = useMemo(() => {
        const normalizedQuery = query.trim().toLowerCase();

        if (!normalizedQuery) {
            return options;
        }

        return options.filter((option: MultiSelectorOptionModel) =>
            option.label.toLowerCase().includes(normalizedQuery),
        );
    }, [options, query]);

    function handleQueryChange(event: ChangeEvent<HTMLInputElement>): void {
        setQuery(event.target.value);
    }

    function toggleOption(id: string): void {
        setSelectedOptionIds((prev: string[]) =>
            prev.includes(id)
                ? prev.filter((optionId: string) => optionId !== id)
                : [...prev, id],
        );
    }

    return {
        options,
        query,
        visibleOptions,
        selectedOptionIds,
        handleQueryChange,
        toggleOption,
    };
}
