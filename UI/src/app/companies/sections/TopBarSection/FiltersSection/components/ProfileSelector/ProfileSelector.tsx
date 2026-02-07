import FilterLabel from "../common/FilterLabel/FilterLabel";
import styles from "./ProfileSelector.module.css";
import { ChevronDown } from "lucide-react";
import CustomButton from "@/app/common/components/Buttons/CustomButton/CustomButton";

export default function ProfileSelector() {
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
