import type { ReactElement } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import SearchBar from "@/app/common/components/SearchBar";
import RecursiveOption from "./components/RecursiveOption/RecursiveOption";
import type { RecursiveNode } from "./models/recursiveNode";
import data from "../../data/locality.json";
import styles from "./styles.module.css";
import Splitter from "@/app/common/components/Splitter/Splitter";
import { useRecursiveSelector } from "./hooks/useRecursiveSelector";
import type { LocalityTree } from "./utils";
import SelectedOptions from "./components/SelectedOptions";
import CustomButton from "@/app/common/components/CustomButton";
import { handleSubmit } from "./utils";

export default function RecursiveSelector(): ReactElement {
    const localityTree: LocalityTree = data;
    const {
        nodes,
        query,
        visibleNodes,
        selectedOptions,
        handleQueryChange,
        toggleOption,
    } = useRecursiveSelector(localityTree);
    const nodeById: Map<string, RecursiveNode> = new Map();

    function collectNodes(items: RecursiveNode[]): void {
        items.forEach((item: RecursiveNode) => {
            nodeById.set(item.id, item);

            if (item.children) {
                collectNodes(item.children);
            }
        });
    }

    collectNodes(nodes);

    const selectedNodes: RecursiveNode[] = selectedOptions.flatMap(
        (id: string): RecursiveNode[] => {
            const node: RecursiveNode | undefined = nodeById.get(id);
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
                {visibleNodes.map((node: RecursiveNode) => (
                    <RecursiveOption
                        key={node.id}
                        node={node}
                        addOption={toggleOption}
                        selectedOptions={selectedOptions}
                        searchQuery={query}
                    />
                ))}
            </div>
            <Splitter />
            <div className={styles.footer}>
                <SelectedOptions
                    selectedNodes={selectedNodes}
                    removeOption={toggleOption}
                />
                <CustomButton
                    onClick={handleSubmit}
                    text="Submit"
                    isPrimary
                    className={styles.submitButton}
                />
            </div>
        </div>
    );
}
