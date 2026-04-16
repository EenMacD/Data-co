"use client";

import { useEffect, useRef, useState, type ReactElement } from "react";
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
    const wrapperRef = useRef<HTMLDivElement>(null);

    const { label, text } = filterData;

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        function handlePointerDown(event: PointerEvent) {
            if (!wrapperRef.current?.contains(event.target as Node)) {
                setIsOpen(false);
            }
        }

        document.addEventListener("pointerdown", handlePointerDown);

        return () => {
            document.removeEventListener("pointerdown", handlePointerDown);
        };
    }, [isOpen]);

    return (
        <div ref={wrapperRef} className={styles.wrapper}>
            <FilterLabel label={label} />
            <CustomButton
                text={text}
                onClick={() => setIsOpen((prev: boolean) => !prev)}
            />
            {isOpen && (
                <div className={styles.popup}>{filterData.template}</div>
            )}
        </div>
    );
}
