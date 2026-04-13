import type { ReactElement } from "react";
import { RefObject } from "react";
import styles from "./styles.module.css";
import CustomScrollbar from "../../../../../common/components/CustomScrollbar";
import CustomButton from "@/app/common/components/CustomButton";

// icons
import {
    ChevronLeft,
    ChevronRight,
    ChevronFirst,
    ChevronLast,
    ChevronsUpDown,
} from "lucide-react";

interface ResultsFooterProps {
    scrollContainerRef: RefObject<HTMLDivElement | null>;
}

export default function ResultsFooter({
    scrollContainerRef,
}: ResultsFooterProps): ReactElement {
    return (
        <div className={styles.footer}>
            <div className={styles.scrollbarContainer}>
                <CustomScrollbar scrollContainerRef={scrollContainerRef} />
            </div>
            <div className={styles.actions}>
                <p className={styles.rowsPerPageText}>Rows Per Page</p>
                <CustomButton
                    leadingIcon={<ChevronsUpDown />}
                    text="5"
                    aria-label="Select rows per page"
                    textStyle={{ fontSize: "1.4rem", fontWeight: "700" }}
                />
                <CustomButton
                    leadingIcon={<ChevronFirst />}
                    aria-label="First page"
                />
                <CustomButton
                    leadingIcon={<ChevronLeft />}
                    aria-label="Previous page"
                />
                <CustomButton
                    text="16"
                    aria-label="Current page 16"
                    textStyle={{ fontSize: "1.4rem", fontWeight: "700" }}
                    buttonStyle={{ padding: "0 2.4rem" }}
                />
                <CustomButton
                    leadingIcon={<ChevronRight />}
                    aria-label="Next page"
                />
                <CustomButton
                    leadingIcon={<ChevronLast />}
                    aria-label="Last page"
                />
            </div>
        </div>
    );
}
