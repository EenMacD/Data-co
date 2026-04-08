"use client";

import {
    ReactElement,
    ReactNode,
    useRef,
    useState,
} from "react";

import styles from "./styles.module.css";

interface Props {
    children: ReactNode;
}

export default function TdContent({ children }: Props): ReactElement {
    const contentRef: { current: HTMLDivElement | null } =
        useRef<HTMLDivElement | null>(null);
    const [showFullText, setShowFullText] = useState(false);

    function updateOverflowState(): void {
        const contentElement: HTMLDivElement | null = contentRef.current;

        if (!contentElement) {
            return;
        }

        const hasOverflow: boolean =
            contentElement.scrollWidth > contentElement.clientWidth;
        setShowFullText(hasOverflow);
    }

    function handleMouseEnter(): void {
        updateOverflowState();
    }

    function handleFocus(): void {
        updateOverflowState();
    }

    function handleMouseLeave(): void {
        setShowFullText(false);
    }

    function handleBlur(): void {
        setShowFullText(false);
    }

    return (
        <td className={styles.td}>
            <div
                className={styles.tdContentWrapper}
                onMouseEnter={handleMouseEnter}
                onMouseLeave={handleMouseLeave}
            >
                <div
                    ref={contentRef}
                    className={`${styles.tdContent} ${showFullText ? styles.expanded : ""}`}
                    tabIndex={0}
                    onFocus={handleFocus}
                    onBlur={handleBlur}
                >
                    {children}
                </div>
            </div>
        </td>
    );
}
