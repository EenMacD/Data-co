import type {
    MultiSelectorOptionInput,
    MultiSelectorOptionModel,
} from "./models/MultiSelectorOptionModel";

export type GetMultiSelectorSubmitValue = (
    option: MultiSelectorOptionModel,
) => string;

export function toMultiSelectorOptions(
    options: MultiSelectorOptionInput[],
): MultiSelectorOptionModel[] {
    return options.map((option: MultiSelectorOptionInput) => {
        if (typeof option === "string") {
            return {
                id: option,
                label: option,
            };
        }

        return option;
    });
}

export function getDefaultMultiSelectorSubmitValue(
    option: MultiSelectorOptionModel,
): string {
    return option.id;
}

export function getOptionIdsForSubmitValues(
    options: MultiSelectorOptionModel[],
    submitValues: string[],
    getSubmitValue: GetMultiSelectorSubmitValue,
): string[] {
    if (!submitValues.length) {
        return [];
    }

    const selectedSubmitValues = new Set(submitValues);

    return options.flatMap((option: MultiSelectorOptionModel) =>
        selectedSubmitValues.has(getSubmitValue(option)) ? [option.id] : [],
    );
}
