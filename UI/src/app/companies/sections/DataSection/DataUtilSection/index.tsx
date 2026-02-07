import SearchBar from "@/app/common/components/SearchBar";
import styles from "./DataUtilSection.module.css";
import CustomButton from "@/app/common/components/Buttons/CustomButton";
import { DownloadIcon, Menu } from "lucide-react";

export default function DataUtilSection() {
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
                <CustomButton leadingIcon={<Menu />} isPrimary />
            </div>
        </div>
    );
}
