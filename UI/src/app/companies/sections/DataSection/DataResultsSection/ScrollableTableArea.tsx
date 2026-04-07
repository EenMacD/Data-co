"use client";

import { useRef } from "react";
import ResultsFooter from "./ResultsFooter";
import styles from "./styles.module.css";

export default function ScrollableTableArea({ 
    children 
}: { 
    children: React.ReactNode 
}) {
    const scrollContainerRef = useRef<HTMLDivElement>(null);

    return (
        <>
            <div className={styles.resultsWrapper} ref={scrollContainerRef}>
                {children}
            </div>
            
            <ResultsFooter scrollContainerRef={scrollContainerRef} />
        </>
    );
}
