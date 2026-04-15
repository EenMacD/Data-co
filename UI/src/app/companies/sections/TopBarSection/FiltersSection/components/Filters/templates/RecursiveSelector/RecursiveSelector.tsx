import type { ReactElement } from "react";
import SearchBar from "@/app/common/components/SearchBar";
import RecursiveOption from "./components/RecursiveOption/RecursiveOption";
import type { RecursiveNodeModel } from "./models/RecursiveNodeModel";
import styles from "./styles.module.css";
import Splitter from "@/app/common/components/Splitter/Splitter";
import { useRecursiveSelector } from "./hooks/useRecursiveSelector";
import type { LocalityTree } from "./utils";
import SelectedOptions from "./components/SelectedOptions";
import CustomButton from "@/app/common/components/CustomButton";
import { handleSubmit } from "./utils";
import type { RecursiveFilterKey } from "@/app/companies/models/search-filter";
import { useAppDispatch } from "@/app/store/hooks";

type RecursiveSelectorProps = {
    data: LocalityTree;
    filterId: RecursiveFilterKey;
};

export default function RecursiveSelector({
    data,
    filterId,
}: RecursiveSelectorProps): ReactElement {
    const dispatch = useAppDispatch();
    const localityTree: LocalityTree = data;
    const {
        nodes,
        query,
        visibleNodes,
        selectedOptions,
        handleQueryChange,
        toggleOption,
    } = useRecursiveSelector(localityTree, filterId);
    const nodeById: Map<string, RecursiveNodeModel> = new Map();

    function collectNodes(items: RecursiveNodeModel[]): void {
        items.forEach((item: RecursiveNodeModel) => {
            nodeById.set(item.id, item);

            if (item.children) {
                collectNodes(item.children);
            }
        });
    }

    collectNodes(nodes);

    const selectedNodes: RecursiveNodeModel[] = selectedOptions.flatMap(
        (id: string): RecursiveNodeModel[] => {
            const node: RecursiveNodeModel | undefined = nodeById.get(id);
            return node ? [node] : [];
        },
    );

    return (
        <div className={styles.selectorCard}>
            <SearchBar
                placeholder="Search Country, City or Town"
                width="100%"
                value={query}
                onChange={handleQueryChange}
            />

            <Splitter />
            <div className={styles.recursiveOptionsWrapper}>
                {visibleNodes.map((node: RecursiveNodeModel) => (
                    <RecursiveOption
                        key={node.id}
                        node={node}
                        addOption={toggleOption}
                        selectedOptions={selectedOptions}
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
                        handleSubmit({
                            selectedValues: selectedNodes.map(
                                (node: RecursiveNodeModel) => node.id,
                            ),
                            filterId,
                        }, dispatch)
                    }
                    text="Submit"
                    isPrimary
                    className={styles.submitButton}
                />
            </div>
        </div>
    );
}
