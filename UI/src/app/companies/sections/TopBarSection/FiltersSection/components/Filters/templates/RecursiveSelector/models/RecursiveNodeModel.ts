export type RecursiveNodeModel = {
    id: string;
    label: string;
    path: string[];
    depth: number;
    children?: RecursiveNodeModel[];
    isMatch?: boolean;
    hasMatchingDescendant?: boolean;
};

export interface RecursiveTree {
    [key: string]: RecursiveTree;
}
