import type { ReactElement } from "react";
import FilterLabel from "../../common/FilterLabel";
import styles from "./styles.module.css";
import CustomButton from "@/app/common/components/Buttons/CustomButton";

interface FilterItemProps {
    label: string;
    text: string;
}

export default function FilterItem({
    label,
    text,
}: FilterItemProps): ReactElement {
    return (
        <div className={styles.container}>
            <FilterLabel label={label} />
            <CustomButton text={text} />
        </div>
    );
}
