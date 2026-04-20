"use client";

import type { ReactElement } from "react";
import { RefObject } from "react";
import styles from "./styles.module.css";
import CustomScrollbar from "../../../../../common/components/CustomScrollbar";
import CustomButton from "@/app/common/components/CustomButton";
import { useAppDispatch, useAppSelector } from "@/app/store/hooks";

// icons
import { ChevronLeft, ChevronRight } from "lucide-react";
import {
    fetchSearchCompanies,
    setLimit,
    setOffset,
} from "@/app/companies/store/features/searchCompaniesSlice";
import RPPSelector from "./components/PageSelector/RPPSelector";
import Splitter from "@/app/common/components/Splitter/Splitter";

interface ResultsFooterProps {
    scrollContainerRef: RefObject<HTMLDivElement | null>;
}

export default function ResultsFooter({
    scrollContainerRef,
}: ResultsFooterProps): ReactElement {
    const dispatch = useAppDispatch();

    const { filters, total } = useAppSelector((state) => state.companiesSearch);
    const limit = filters.limit ?? 0;
    const offset = filters.offset ?? 0;

    const currentPage = limit > 0 ? Math.floor(offset / limit) + 1 : 1;
    const hasPreviousPage = currentPage > 1;
    const hasNextPage = limit > 0 && offset + limit < total;

    function handlePreviousPage(): void {
        const prevOffset = Math.max(0, offset - limit);
        dispatch(setOffset(prevOffset));
        void dispatch(fetchSearchCompanies());
    }

    function handleNextPage(): void {
        if (!hasNextPage) {
            return;
        }

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
                <RPPSelector
                    limit={limit}
                    handleChangeAction={handleChangeLimit}
                />

                <Splitter isVertical />

                {/* <CustomButton
                    leadingIcon={<ChevronFirst />}
                    aria-label="First page"
                /> */}
                {hasPreviousPage && (
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
                {hasNextPage && (
                    <CustomButton
                        leadingIcon={<ChevronRight />}
                        aria-label="Next page"
                        onClick={handleNextPage}
                    />
                )}
                {/* <CustomButton
                    leadingIcon={<ChevronLast />}
                    aria-label="Last page"
                /> */}
            </div>
        </div>
    );
}
