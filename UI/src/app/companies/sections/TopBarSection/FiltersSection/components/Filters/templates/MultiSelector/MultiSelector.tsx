import { type CSSProperties, type ReactElement, useMemo } from "react";
import { Check, Square, X } from "lucide-react";
import CustomButton from "@/app/common/components/CustomButton";
import SearchBar from "@/app/common/components/SearchBar";
import Splitter from "@/app/common/components/Splitter/Splitter";
import { useMultiSelector } from "./hooks/useMultiSelector";
import type {
    MultiSelectorOptionInput,
    MultiSelectorOptionModel,
} from "./models/MultiSelectorOptionModel";
import {
    getDefaultMultiSelectorSubmitValue,
    type GetMultiSelectorSubmitValue,
} from "./utils";
import styles from "./styles.module.css";

type MultiSelectorProps = {
    data: MultiSelectorOptionInput[];
    selectedValues: string[];
    searchPlaceholder: string;
    onApply: (selectedValues: string[]) => void;
    getSubmitValue?: GetMultiSelectorSubmitValue;
    submitOnOptionSelect?: boolean;
    onClose?: () => void;
    cardStyles?: CSSProperties;
};

export default function MultiSelector({
    data,
    selectedValues,
    searchPlaceholder,
    onApply,
    getSubmitValue = getDefaultMultiSelectorSubmitValue,
    submitOnOptionSelect = false,
    onClose,
    cardStyles,
}: MultiSelectorProps): ReactElement {
    const {
        options,
        query,
        visibleOptions,
        selectedOptionIds,
        handleQueryChange,
        toggleOption,
    } = useMultiSelector({
        data,
        selectedValues,
        getSubmitValue,
    });
    const optionById: Map<string, MultiSelectorOptionModel> = useMemo(
        () =>
            new Map(
                options.map((option: MultiSelectorOptionModel) => [
                    option.id,
                    option,
                ]),
            ),
        [options],
    );

    function getSelectedOptions(
        optionIds: string[],
    ): MultiSelectorOptionModel[] {
        return optionIds.flatMap((id: string) => {
            const option = optionById.get(id);

            return option ? [option] : [];
        });
    }

    function submitOptions(optionIds: string[]): void {
        onApply(
            Array.from(
                new Set(getSelectedOptions(optionIds).map(getSubmitValue)),
            ),
        );
    }

    function handleOptionToggle(id: string): void {
        const nextSelectedOptionIds: string[] = selectedOptionIds.includes(id)
            ? selectedOptionIds.filter((optionId: string) => optionId !== id)
            : [...selectedOptionIds, id];

        toggleOption(id);

        if (submitOnOptionSelect) {
            submitOptions(nextSelectedOptionIds);
        }
    }

    function handleButtonSubmit(): void {
        submitOptions(selectedOptionIds);
        onClose?.();
    }

    const selectedOptions: MultiSelectorOptionModel[] =
        getSelectedOptions(selectedOptionIds);

    return (
        <div style={cardStyles} className={styles.selectorCard}>
            <SearchBar
                placeholder={searchPlaceholder}
                width="100%"
                value={query}
                onChange={handleQueryChange}
            />

            <Splitter />
            <div className={styles.optionsWrapper}>
                {visibleOptions.map((option: MultiSelectorOptionModel) => {
                    const isSelected = selectedOptionIds.includes(option.id);

                    return (
                        <button
                            key={option.id}
                            type="button"
                            className={styles.option}
                            onClick={() => handleOptionToggle(option.id)}
                        >
                            <span className={styles.iconWrapper}>
                                <Square
                                    className={
                                        isSelected
                                            ? styles.selectedSquare
                                            : styles.square
                                    }
                                />
                                {isSelected && (
                                    <Check className={styles.check} />
                                )}
                            </span>
                            <h4 className={styles.label}>{option.label}</h4>
                        </button>
                    );
                })}
            </div>

            <div className={styles.selectedOptionsCard}>
                {selectedOptions.map((option: MultiSelectorOptionModel) => (
                    <button
                        key={option.id}
                        type="button"
                        className={styles.tag}
                        onClick={() => handleOptionToggle(option.id)}
                        aria-label={`Remove ${option.label}`}
                    >
                        <span className={styles.tagLabel}>{option.label}</span>
                        <X className={styles.tagRemove} />
                    </button>
                ))}
            </div>

            {!submitOnOptionSelect && (
                <CustomButton
                    onClick={handleButtonSubmit}
                    text="Submit"
                    isPrimary
                    className={styles.submitButton}
                />
            )}
        </div>
    );
}
