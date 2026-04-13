"use client";

import { useState, type ReactElement } from "react";
import FilterLabel from "../../../common/FilterLabel";
import styles from "./styles.module.css";
import CustomButton from "@/app/common/components/CustomButton";
import PinnedFilter from "../../models/pinnedFilters";

export default function FilterItem({
    filterData,
}: {
    filterData: PinnedFilter;
}): ReactElement {
    const [isOpen, setIsOpen] = useState(false);

    const { label, text } = filterData;

    return (
        <div className={styles.wrapper}>
            <FilterLabel label={label} />
            <CustomButton
                text={text}
                onClick={() => setIsOpen((prev) => !prev)}
            />
            {isOpen && (
                <div className={styles.popup}>{filterData.template}</div>
            )}
        </div>
    );
}
