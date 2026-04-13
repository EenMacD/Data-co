import type { ReactElement } from "react";
import SearchBar from "@/app/common/components/SearchBar";
import styles from "./styles.module.css";
import CustomButton from "@/app/common/components/CustomButton";
import { DownloadIcon, Menu } from "lucide-react";

export default function DataUtilSection(): ReactElement {
    return (
        <div className={styles.DataUtilSection}>
            <div className={styles.searchWrapper}>
                <SearchBar
                    placeholder="Search Results"
                    width="28rem"
                    height="4.8rem"
                />
                <p className={styles.resutlsText}>242,384 Results</p>
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
