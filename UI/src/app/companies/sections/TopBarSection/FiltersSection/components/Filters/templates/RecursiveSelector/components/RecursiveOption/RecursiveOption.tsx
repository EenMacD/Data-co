import type { ReactElement } from "react";
import { useState } from "react";
import type { RecursiveNodeModel } from "../../models/RecursiveNodeModel";
import RecursiveContent from "./RecursiveContent/RecursiveContent";
import styles from "./styles.module.css";

type RecursiveOptionProps = {
    node: RecursiveNodeModel;
    addOption: (id: string) => void;
    selectedOptions: string[];
    disableFirstNodeSelection?: boolean;
    searchQuery?: string;
};

export default function RecursiveOption({
    node,
    addOption,
    selectedOptions,
    disableFirstNodeSelection = false,
    searchQuery = "",
}: RecursiveOptionProps): ReactElement {
    const [isExpanded, setIsExpanded] = useState(false);
    // Auto-open only the path that leads to a deeper match.
    const shouldAutoExpand: boolean =
        searchQuery.trim().length > 0 && Boolean(node.hasMatchingDescendant);
    const isNodeExpanded: boolean = shouldAutoExpand || isExpanded;
    const isLast: boolean = !node.children;
    const isSelectable: boolean = !(
        disableFirstNodeSelection && node.depth === 0
    );
    const isSelected: boolean = isSelectable && selectedOptions.includes(node.id);

    return (
        <div
            className={`${styles.recursiveOption} ${isLast && styles.lastOption}`}
        >
            <RecursiveContent
                id={node.id}
                label={node.label}
                depth={node.depth}
                isLast={isLast}
                isExpanded={isNodeExpanded}
                setIsExpanded={setIsExpanded}
                addOption={addOption}
                isSelected={isSelected}
                isSelectable={isSelectable}
                searchQuery={searchQuery}
            />
            {isNodeExpanded &&
                node.children?.map((child: RecursiveNodeModel) => (
                    <RecursiveOption
                        key={child.id}
                        node={child}
                        addOption={addOption}
                        selectedOptions={selectedOptions}
                        disableFirstNodeSelection={disableFirstNodeSelection}
                        searchQuery={searchQuery}
                    ></RecursiveOption>
                ))}
        </div>
    );
}
