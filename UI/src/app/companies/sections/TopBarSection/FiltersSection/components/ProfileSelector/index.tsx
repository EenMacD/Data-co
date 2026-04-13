import type { ReactElement } from "react";
import FilterLabel from "../common/FilterLabel";
import styles from "./styles.module.css";
import { ChevronDown } from "lucide-react";
import CustomButton from "@/app/common/components/CustomButton";

export default function ProfileSelector(): ReactElement {
    return (
        <div className={styles.container}>
            <FilterLabel label="Profile" />
            <div className={styles.wrapper}>
                <CustomButton
                    text="default profile"
                    trailingIcon={<ChevronDown />}
                    isRound={true}
                />
                <div className={styles.divider}></div>
            </div>
        </div>
    );
}
