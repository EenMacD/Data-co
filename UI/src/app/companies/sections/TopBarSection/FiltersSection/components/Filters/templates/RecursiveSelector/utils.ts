import type { RecursiveNodeModel } from "./models/RecursiveNodeModel";
import { SelectedNodesRequestModel } from "./models/SelectedNodesRequest";
import {
    fetchSearchCompanies,
    setRecursiveFilter,
} from "@/app/companies/store/features/searchCompaniesSlice";
import type { AppDispatch } from "@/app/store/store";

export interface LocalityTree {
    [key: string]: LocalityTree;
}

export function toRecursiveNodes(
    tree: LocalityTree,
    parentPath: string[] = [],
    depth: number = 0,
): RecursiveNodeModel[] {
    // Convert the raw object tree into renderable nodes.
    return Object.entries(tree).map(
        ([label, children]: [string, LocalityTree]) => {
            const path: string[] = [...parentPath, label];
            const childNodes: RecursiveNodeModel[] = toRecursiveNodes(
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

export function handleSubmit(
    request: SelectedNodesRequestModel,
    dispatch: AppDispatch,
): void {
    dispatch(
        setRecursiveFilter({
            selectedValues: request.selectedValues,
            filterKey: request.filterId,
        }),
    );
    void dispatch(fetchSearchCompanies());
}
