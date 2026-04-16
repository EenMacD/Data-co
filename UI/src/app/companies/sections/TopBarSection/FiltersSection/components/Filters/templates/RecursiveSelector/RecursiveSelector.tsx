import SearchBar from "@/app/common/components/SearchBar";
import RecursiveOption from "./components/RecursiveOption/RecursiveOption";
import type {
    RecursiveNodeModel,
    RecursiveTree,
} from "./models/RecursiveNodeModel";
import styles from "./styles.module.css";
import Splitter from "@/app/common/components/Splitter/Splitter";
import { useRecursiveSelector } from "./hooks/useRecursiveSelector";
import type { GetRecursiveNodeSubmitValue } from "./utils";
import SelectedOptions from "./components/SelectedOptions";
import CustomButton from "@/app/common/components/CustomButton";
import {
    createRecursiveNodeLookup,
    getDefaultRecursiveNodeSubmitValue,
} from "./utils";
import { type CSSProperties, type ReactElement, useMemo } from "react";

type RecursiveSelectorProps = {
    data: RecursiveTree;
    selectedValues: string[];
    searchPlaceholder: string;
    onApply: (selectedValues: string[]) => void;
    getSubmitValue?: GetRecursiveNodeSubmitValue;
    disableFirstNodeSelection?: boolean;
    submitOnOptionSelect?: boolean;
    onClose?: () => void;
    cardStyles?: CSSProperties;
};

export default function RecursiveSelector({
    data,
    selectedValues,
    searchPlaceholder,
    onApply,
    getSubmitValue = getDefaultRecursiveNodeSubmitValue,
    disableFirstNodeSelection = false,
    submitOnOptionSelect = false,
    onClose,
    cardStyles,
}: RecursiveSelectorProps): ReactElement {
    const {
        nodes,
        query,
        visibleNodes,
        selectedOptions,
        handleQueryChange,
        toggleOption,
    } = useRecursiveSelector({
        tree: data,
        selectedValues,
        getSubmitValue,
    });
    const nodeById: Map<string, RecursiveNodeModel> = useMemo(
        () => createRecursiveNodeLookup(nodes),
        [nodes],
    );

    function getSelectedNodes(optionIds: string[]): RecursiveNodeModel[] {
        return optionIds.flatMap((id) => {
            const node = nodeById.get(id);
            if (!node || (disableFirstNodeSelection && node.depth === 0)) {
                return [];
            }

            return [node];
        });
    }

    function submitOptions(optionIds: string[]): void {
        onApply(
            Array.from(
                new Set(getSelectedNodes(optionIds).map(getSubmitValue)),
            ),
        );
    }

    function handleButtonSubmit(): void {
        submitOptions(selectedOptions);
        onClose?.();
    }

    function handleOptionToggle(id: string): void {
        const nextSelectedOptions: string[] = selectedOptions.includes(id)
            ? selectedOptions.filter((optionId: string) => optionId !== id)
            : [...selectedOptions, id];

        toggleOption(id);

        if (submitOnOptionSelect) {
            submitOptions(nextSelectedOptions);
        }
    }

    const selectedNodes = getSelectedNodes(selectedOptions);

    return (
        <div style={cardStyles} className={styles.selectorCard}>
            <SearchBar
                placeholder={searchPlaceholder}
                width="100%"
                value={query}
                onChange={handleQueryChange}
            />

            <Splitter />
            <div className={styles.recursiveOptionsWrapper}>
                {visibleNodes.map((node) => (
                    <RecursiveOption
                        key={node.id}
                        node={node}
                        addOption={handleOptionToggle}
                        selectedOptions={selectedOptions}
                        disableFirstNodeSelection={disableFirstNodeSelection}
                        searchQuery={query}
                    />
                ))}
            </div>
            <div className={styles.footer}>
                <SelectedOptions
                    selectedNodes={selectedNodes}
                    removeOption={handleOptionToggle}
                />
                {!submitOnOptionSelect && (
                    <CustomButton
                        onClick={handleButtonSubmit}
                        text="Submit"
                        isPrimary
                        className={styles.submitButton}
                    />
                )}
            </div>
        </div>
    );
}
