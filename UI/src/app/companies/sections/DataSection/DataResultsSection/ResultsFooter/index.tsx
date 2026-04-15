"use client";

import type { ReactElement } from "react";
import { RefObject } from "react";
import styles from "./styles.module.css";
import CustomScrollbar from "../../../../../common/components/CustomScrollbar";
import CustomButton from "@/app/common/components/CustomButton";
import { useAppDispatch, useAppSelector } from "@/app/store/hooks";

// icons
import { ChevronLeft, ChevronRight, ChevronsUpDown } from "lucide-react";
import {
    fetchSearchCompanies,
    setLimit,
    setOffset,
} from "@/app/companies/store/features/searchCompaniesSlice";
import PageSelector from "./components/PageSelector/PageSelector";

interface ResultsFooterProps {
    scrollContainerRef: RefObject<HTMLDivElement | null>;
}

export default function ResultsFooter({
    scrollContainerRef,
}: ResultsFooterProps): ReactElement {
    const dispatch = useAppDispatch();

    const { filters } = useAppSelector((state) => state.companiesSearch);
    const limit = filters.limit ?? 0;
    const offset = filters.offset ?? 0;

    const currentPage = limit > 0 ? Math.floor(offset / limit) + 1 : 1;

    function handlePreviousPage(): void {
        const prevOffset = Math.max(0, offset - limit);
        dispatch(setOffset(prevOffset));
        void dispatch(fetchSearchCompanies());
    }

    function handleNextPage(): void {
        const nextOffset = offset + limit;
        dispatch(setOffset(nextOffset));
        void dispatch(fetchSearchCompanies());
    }

    function handleChangeLimit(newLimit: number): void {
        dispatch(setLimit(newLimit));
        dispatch(setOffset(0));
        void dispatch(fetchSearchCompanies());
    }

    return (
        <div className={styles.footer}>
            <div className={styles.scrollbarContainer}>
                <CustomScrollbar scrollContainerRef={scrollContainerRef} />
            </div>
            <div className={styles.actions}>
                <p className={styles.rowsPerPageText}>Rows Per Page</p>
                <PageSelector limit={5} handleChange={handleChangeLimit} />

                {/* <CustomButton
                    leadingIcon={<ChevronFirst />}
                    aria-label="First page"
                /> */}
                {currentPage > 1 && (
                    <CustomButton
                        leadingIcon={<ChevronLeft />}
                        aria-label="Previous page"
                        onClick={handlePreviousPage}
                    />
                )}
                <CustomButton
                    text={currentPage.toString()}
                    aria-label={`Current page ${currentPage}`}
                    textStyle={{ fontSize: "1.4rem", fontWeight: "700" }}
                    buttonStyle={{ padding: "0 2.4rem" }}
                />
                <CustomButton
                    leadingIcon={<ChevronRight />}
                    aria-label="Next page"
                    onClick={handleNextPage}
                />
                {/* <CustomButton
                    leadingIcon={<ChevronLast />}
                    aria-label="Last page"
                /> */}
            </div>
        </div>
    );
}
