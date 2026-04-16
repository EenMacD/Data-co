import type {
    Dispatch,
    MouseEvent as ReactMouseEvent,
    ReactElement,
    SetStateAction,
} from "react";
import { ChevronDown, ChevronUp, Square, Check } from "lucide-react";
import styles from "./styles.module.css";

type RecursiveContentProps = {
    id: string;
    label: string;
    depth: number;
    isLast: boolean;
    isSelected: boolean;
    isSelectable: boolean;
    isExpanded: boolean;
    setIsExpanded: Dispatch<SetStateAction<boolean>>;
    addOption: (id: string) => void;
    searchQuery?: string;
};

export default function RecursiveContent({
    id,
    label,
    depth,
    isLast,
    isSelected,
    isSelectable,
    isExpanded,
    setIsExpanded,
    addOption,
    searchQuery = "",
}: RecursiveContentProps): ReactElement {
    function handleToggle(event: ReactMouseEvent<HTMLSpanElement>): void {
        event.stopPropagation();
        if (isLast) {
            return;
        }

        setIsExpanded((prev: boolean) => !prev);
    }

    function handleClick(): void {
        if (isSelectable) {
            addOption(id);
            return;
        }

        if (!isLast) {
            setIsExpanded((prev: boolean) => !prev);
        }
    }

    function renderHighlightedLabel(): ReactElement | string {
        const normalizedQuery: string = searchQuery.trim().toLowerCase();

        if (!normalizedQuery) {
            return label;
        }

        const matchIndex: number = label.toLowerCase().indexOf(normalizedQuery);

        if (matchIndex === -1) {
            return label;
        }

        const matchEnd: number = matchIndex + normalizedQuery.length;

        return (
            <>
                {label.slice(0, matchIndex)}
                {/* Highlight only the matched part of the label. */}
                <span className={styles.highlight}>
                    {label.slice(matchIndex, matchEnd)}
                </span>
                {label.slice(matchEnd)}
            </>
        );
    }

    return (
        <div
            style={{ marginLeft: `${depth * 12}px` }}
            className={`${styles.contentWrapper} ${isLast && styles.lastOption} ${!isSelectable && styles.notSelectable}`}
            onClick={handleClick}
        >
            <span
                className={styles.arrowWrapper}
                aria-hidden="true"
                onClick={handleToggle}
            >
                {!isLast &&
                    (isExpanded ? (
                        <ChevronUp className={styles.arrow} />
                    ) : (
                        <ChevronDown className={styles.arrow} />
                    ))}
            </span>
            {isSelectable && (
                <span className={styles.iconWrapper}>
                    <Square
                        className={
                            isSelected ? styles.selectedSquare : styles.square
                        }
                    />
                    {isSelected && <Check className={styles.check} />}
                </span>
            )}
            <h4 className={styles.label}>{renderHighlightedLabel()}</h4>
        </div>
    );
}
