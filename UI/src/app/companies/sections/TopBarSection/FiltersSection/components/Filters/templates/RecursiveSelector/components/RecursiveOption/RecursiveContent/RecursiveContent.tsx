import styles from "./styles.module.css";
import { ChevronDown, ChevronUp, Square, SquareCheck } from "lucide-react";

type RecursiveContentProps = {
    label: string;
    depth: number;
    isLast: boolean;
    isExpanded: boolean;
    setIsExpanded: React.Dispatch<React.SetStateAction<boolean>>;
};

export default function RecursiveContent({
    label,
    depth,
    isLast,
    isExpanded,
    setIsExpanded,
}: RecursiveContentProps) {
    return (
        <div
            style={{ marginLeft: `${depth * 12}px` }}
            className={styles.wrapper}
            onClick={() => setIsExpanded((prev) => !prev)}
        >
            {!isLast && (isExpanded == true ? <ChevronUp /> : <ChevronDown />)}
            <Square />
            <div>{label}</div>
        </div>
    );
}
