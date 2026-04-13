export type RecursiveNode = {
    id: string;
    label: string;
    path: string[];
    depth: number;
    children?: RecursiveNode[];
    isMatch?: boolean;
    hasMatchingDescendant?: boolean;
};
