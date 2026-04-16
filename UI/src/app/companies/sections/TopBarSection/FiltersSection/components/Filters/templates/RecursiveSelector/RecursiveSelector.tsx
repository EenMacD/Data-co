import SearchBar from "@/app/common/components/SearchBar";
import RecursiveOption from "./components/RecursiveOption/RecursiveOption";
import type { RecursiveNodeModel } from "./models/RecursiveNodeModel";
import styles from "./styles.module.css";
import Splitter from "@/app/common/components/Splitter/Splitter";
import { useRecursiveSelector } from "./hooks/useRecursiveSelector";
import type { LocalityTree } from "./utils";
import SelectedOptions from "./components/SelectedOptions";
import CustomButton from "@/app/common/components/CustomButton";
import { getRecursiveNodeSubmitValue, handleSubmit } from "./utils";
import type { RecursiveFilterKey } from "@/app/companies/models/search-filter";
import { useAppDispatch } from "@/app/store/hooks";

type RecursiveSelectorProps = {
    data: LocalityTree;
    filterId: RecursiveFilterKey;
    searchPlaceholder?: string;
    disableFirstNodeSelection?: boolean;
};

export default function RecursiveSelector({
    data,
    filterId,
    searchPlaceholder = "Search Country, City or Town",
    disableFirstNodeSelection = false,
}: RecursiveSelectorProps) {
    const dispatch = useAppDispatch();
    const localityTree = data;
    const {
        nodes,
        query,
        visibleNodes,
        selectedOptions,
        handleQueryChange,
        toggleOption,
    } = useRecursiveSelector(localityTree, filterId);
    const nodeById: Map<string, RecursiveNodeModel> = new Map();

    function collectNodes(items: RecursiveNodeModel[]) {
        items.forEach((item) => {
            nodeById.set(item.id, item);

            if (item.children) {
                collectNodes(item.children);
            }
        });
    }

    collectNodes(nodes);

    const selectedNodes = selectedOptions.flatMap((id) => {
        const node = nodeById.get(id);
        if (!node || (disableFirstNodeSelection && node.depth === 0)) {
            return [];
        }

        return [node];
    });

    return (
        <div className={styles.selectorCard}>
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
                        addOption={toggleOption}
                        selectedOptions={selectedOptions}
                        disableFirstNodeSelection={disableFirstNodeSelection}
                        searchQuery={query}
                    />
                ))}
            </div>
            <div className={styles.footer}>
                <SelectedOptions
                    selectedNodes={selectedNodes}
                    removeOption={toggleOption}
                />
                <CustomButton
                    onClick={() =>
                        handleSubmit(
                            {
                                selectedValues: Array.from(
                                    new Set(
                                        selectedNodes.map(
                                            getRecursiveNodeSubmitValue,
                                        ),
                                    ),
                                ),
                                filterId,
                            },
                            dispatch,
                        )
                    }
                    text="Submit"
                    isPrimary
                    className={styles.submitButton}
                />
            </div>
        </div>
    );
}
