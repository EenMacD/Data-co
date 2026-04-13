import type { RecursiveNode } from "./models/recursiveNode";
import type { MouseEvent as ReactMouseEvent } from "react";

export interface LocalityTree {
    [key: string]: LocalityTree;
}

export function toRecursiveNodes(
    tree: LocalityTree,
    parentPath: string[] = [],
    depth: number = 0,
): RecursiveNode[] {
    // Convert the raw object tree into renderable nodes.
    return Object.entries(tree).map(
        ([label, children]: [string, LocalityTree]) => {
            const path: string[] = [...parentPath, label];
            const childNodes: RecursiveNode[] = toRecursiveNodes(
                children,
                path,
                depth + 1,
            );

            return {
                id: path.join("/"),
                label,
                path,
                depth,
                children: childNodes.length ? childNodes : undefined,
            };
        },
    );
}

export function filterRecursiveNodes(
    nodes: RecursiveNode[],
    query: string,
): RecursiveNode[] {
    const normalizedQuery: string = query.trim().toLowerCase();

    if (!normalizedQuery) {
        return nodes;
    }

    return nodes.flatMap((node: RecursiveNode) => {
        const filteredChildren: RecursiveNode[] | undefined = node.children
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

export function handleSubmit(
    event: ReactMouseEvent<HTMLButtonElement, MouseEvent>,
): void {}
