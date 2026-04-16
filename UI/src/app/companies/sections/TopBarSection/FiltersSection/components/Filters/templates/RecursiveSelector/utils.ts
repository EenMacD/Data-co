import type {
    RecursiveNodeModel,
    RecursiveTree,
} from "./models/RecursiveNodeModel";

export type GetRecursiveNodeSubmitValue = (node: RecursiveNodeModel) => string;

export function toRecursiveNodes(
    tree: RecursiveTree,
    parentPath: string[] = [],
    parentIndexPath: number[] = [],
    depth: number = 0,
): RecursiveNodeModel[] {
    // Convert the raw object tree into renderable nodes.
    return Object.entries(tree).map(
        ([label, children]: [string, RecursiveTree], index: number) => {
            const path: string[] = [...parentPath, label];
            const indexPath: number[] = [...parentIndexPath, index];
            const childNodes: RecursiveNodeModel[] = toRecursiveNodes(
                children,
                path,
                indexPath,
                depth + 1,
            );

            return {
                id: indexPath.join("."),
                label,
                path,
                depth,
                children: childNodes.length ? childNodes : undefined,
            };
        },
    );
}

export function createRecursiveNodeLookup(
    nodes: RecursiveNodeModel[],
): Map<string, RecursiveNodeModel> {
    const nodeById = new Map<string, RecursiveNodeModel>();

    function collectNodes(items: RecursiveNodeModel[]): void {
        items.forEach((item: RecursiveNodeModel) => {
            nodeById.set(item.id, item);

            if (item.children) {
                collectNodes(item.children);
            }
        });
    }

    collectNodes(nodes);

    return nodeById;
}

export function getDefaultRecursiveNodeSubmitValue(
    node: RecursiveNodeModel,
): string {
    return node.id;
}

export function getNodeIdsForSubmitValues(
    nodes: RecursiveNodeModel[],
    submitValues: string[],
    getSubmitValue: GetRecursiveNodeSubmitValue,
): string[] {
    if (!submitValues.length) {
        return [];
    }

    const submitValueSet: Set<string> = new Set(submitValues);
    const ids: string[] = [];

    function collectMatchingIds(items: RecursiveNodeModel[]): void {
        items.forEach((node: RecursiveNodeModel) => {
            if (submitValueSet.has(getSubmitValue(node))) {
                ids.push(node.id);
            }

            if (node.children) {
                collectMatchingIds(node.children);
            }
        });
    }

    collectMatchingIds(nodes);

    return ids;
}

export function filterRecursiveNodes(
    nodes: RecursiveNodeModel[],
    query: string,
): RecursiveNodeModel[] {
    const normalizedQuery: string = query.trim().toLowerCase();

    if (!normalizedQuery) {
        return nodes;
    }

    return nodes.flatMap((node: RecursiveNodeModel) => {
        const filteredChildren: RecursiveNodeModel[] | undefined = node.children
            ? filterRecursiveNodes(node.children, normalizedQuery)
            : undefined;
        // Match against the current label only.
        const matchesNode: boolean = node.label
            .toLowerCase()
            .includes(normalizedQuery);
        // Keep parents when a deeper child matches.
        const hasMatchingDescendant: boolean = Boolean(
            filteredChildren?.length,
        );

        if (!matchesNode && !hasMatchingDescendant) {
            return [];
        }

        return [
            {
                ...node,
                // Direct matches keep their real children for manual expansion.
                children: hasMatchingDescendant
                    ? filteredChildren
                    : matchesNode
                      ? node.children
                      : undefined,
                isMatch: matchesNode,
                hasMatchingDescendant,
            },
        ];
    });
}
