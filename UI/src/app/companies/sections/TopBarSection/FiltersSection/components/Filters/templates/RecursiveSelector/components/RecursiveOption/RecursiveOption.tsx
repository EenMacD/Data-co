import { useState } from "react";
import { RecursiveNode } from "../../models/recursiveNode";
import RecursiveContent from "./RecursiveContent/RecursiveContent";

type RecursiveOptionProps = {
    node: RecursiveNode;
};

export default function RecursiveOption({ node }: RecursiveOptionProps) {
    const [isExpanded, setIsExpanded] = useState(false);

    return (
        <div>
            <RecursiveContent
                label={node.label}
                depth={node.depth}
                isLast={!node.children}
                isExpanded={isExpanded}
                setIsExpanded={setIsExpanded}
            />
            {isExpanded &&
                node.children?.map((child) => (
                    <RecursiveOption
                        key={child.id}
                        node={child}
                    ></RecursiveOption>
                ))}
        </div>
    );
}
