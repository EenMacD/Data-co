export type RecursiveNodeModel = {
    id: string;
    label: string;
    path: string[];
    depth: number;
    children?: RecursiveNodeModel[];
    isMatch?: boolean;
    hasMatchingDescendant?: boolean;
};
