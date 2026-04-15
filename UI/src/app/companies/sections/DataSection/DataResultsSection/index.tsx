"use client";

import { useEffect, type ReactElement } from "react";
import ScrollableTableArea from "./ScrollableTableArea";
import ResultsHeader from "./ResultsHeader";
import ResultsBody from "./ResultsBody";
import styles from "./styles.module.css";
import { useAppDispatch, useAppSelector } from "@/app/store/hooks";
import { fetchSearchCompanies } from "../../../store/features/searchCompaniesSlice";

export default function DataResultsSection(): ReactElement {
    const dispatch = useAppDispatch();
    const {
        companies = [],
        error,
        status,
    } = useAppSelector((state) => state.companiesSearch);

    useEffect(() => {
        if (status === "idle") {
            void dispatch(fetchSearchCompanies());
        }
    }, [dispatch, status]);

    const emptyMessage: string =
        status === "loading"
            ? "Loading companies..."
            : (error ?? "No data available.");

    const columns: string[] = [
        "Company Name",
        "Company Number",
        "Locality",
        "Status",
        // "SIC Code",
        // "Turnover",
        // "Officers",
        // "Date",
    ];

    return (
        <div className={styles.container}>
            <ScrollableTableArea>
                <table className={styles.table}>
                    <colgroup>
                        <col className={styles.firstColumn} />
                        {columns.slice(1).map(
                            (_column: string, index: number): ReactElement => (
                                <col key={index} />
                            ),
                        )}
                    </colgroup>
                    <ResultsHeader columns={columns} />
                    <ResultsBody data={companies} emptyMessage={emptyMessage} />
                </table>
            </ScrollableTableArea>
        </div>
    );
}
