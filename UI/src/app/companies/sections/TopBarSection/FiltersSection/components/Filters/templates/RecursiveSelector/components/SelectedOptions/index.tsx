import type { ReactElement } from "react";
import type { RecursiveNode } from "../../models/recursiveNode";
import { X } from "lucide-react";
import styles from "./style.module.css";

type SelectedOptionsProps = {
    selectedNodes: RecursiveNode[];
    removeOption: (id: string) => void;
};

export default function SelectedOptions({
    selectedNodes,
    removeOption,
}: SelectedOptionsProps): ReactElement {
    return (
        <div className={styles.SelectedOptionsCard}>
            {selectedNodes.map((node: RecursiveNode) => (
                <button
                    key={node.id}
                    type="button"
                    className={styles.tag}
                    onClick={() => removeOption(node.id)}
                    aria-label={`Remove ${node.label}`}
                    title={node.path.join(" / ")}
                >
                    <span className={styles.tagLabel}>{node.label}</span>

                    <X className={styles.tagRemove} />
                </button>
            ))}
        </div>
    );
}
