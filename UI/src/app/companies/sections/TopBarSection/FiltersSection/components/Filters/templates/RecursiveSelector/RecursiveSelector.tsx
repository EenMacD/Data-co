import SearchBar from "@/app/common/components/SearchBar";
import RecursiveOption from "./components/RecursiveOption/RecursiveOption";
import type { RecursiveNode } from "./models/recursiveNode";
import data from "../../data/locality.json";

interface LocalityTree {
    [key: string]: LocalityTree;
}

function toRecursiveNodes(
    tree: LocalityTree,
    parentPath: string[] = [],
    depth = 0,
): RecursiveNode[] {
    return Object.entries(tree).map(([label, children]) => {
        const path = [...parentPath, label];
        const childNodes = toRecursiveNodes(children, path, depth + 1);

        return {
            id: path.join("/"),
            label,
            path,
            depth,
            children: childNodes.length ? childNodes : undefined,
        };
    });
}

export default function RecursiveSelector() {
    const nodes: RecursiveNode[] = toRecursiveNodes(data as LocalityTree);

    return (
        <div>
            <SearchBar
                placeholder="Search Country, City or Town"
                width="100%"
            ></SearchBar>
            {nodes.map((node) => (
                <RecursiveOption key={node.id} node={node} />
            ))}
        </div>
    );
}
