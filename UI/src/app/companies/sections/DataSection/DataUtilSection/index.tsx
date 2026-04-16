"use client";

import type { ReactElement } from "react";
import SearchBar from "@/app/common/components/SearchBar";
import styles from "./styles.module.css";
import CustomButton from "@/app/common/components/CustomButton";
import { DownloadIcon, Menu } from "lucide-react";
import { useAppSelector } from "@/app/store/hooks";

export default function DataUtilSection(): ReactElement {
    const { total } = useAppSelector((state) => state.companiesSearch);
    const resultsCount: number = total;

    return (
        <div className={styles.DataUtilSection}>
            <div className={styles.searchWrapper}>
                <SearchBar
                    placeholder="Search Results"
                    width="28rem"
                    height="4.8rem"
                />
                <p className={styles.resutlsText}>
                    {resultsCount.toLocaleString("en-GB")} Results
                </p>
            </div>

            <div className={styles.utilWrapper}>
                <CustomButton leadingIcon={<DownloadIcon />} text="Export" />
                <CustomButton
                    leadingIcon={<Menu />}
                    isPrimary
                    aria-label="Open menu"
                />
            </div>
        </div>
    );
}
