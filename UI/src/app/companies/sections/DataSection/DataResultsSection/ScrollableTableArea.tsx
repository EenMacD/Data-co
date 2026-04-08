"use client";

import type { ReactElement, ReactNode, RefObject } from "react";
import { useRef } from "react";
import ResultsFooter from "./ResultsFooter";
import styles from "./styles.module.css";

interface ScrollableTableAreaProps {
    children: ReactNode;
}

export default function ScrollableTableArea({
    children,
}: ScrollableTableAreaProps): ReactElement {
    const scrollContainerRef: RefObject<HTMLDivElement | null> =
        useRef<HTMLDivElement>(null);

    return (
        <>
            <div className={styles.resultsWrapper} ref={scrollContainerRef}>
                {children}
            </div>

            <ResultsFooter scrollContainerRef={scrollContainerRef} />
        </>
    );
}
