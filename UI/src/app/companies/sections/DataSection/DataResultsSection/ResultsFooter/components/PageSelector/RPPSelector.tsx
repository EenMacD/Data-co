"use client";

import CustomButton from "@/app/common/components/CustomButton";
import { ChevronsUpDown } from "lucide-react";
import { useEffect, useRef, useState, type ReactElement } from "react";
import styles from "./PageSelector.module.css";

type RPPSelectorProps = {
    limit: number;
    handleChangeAction: (value: number) => void;
};

const ROWS_PER_PAGE_OPTIONS = [10, 20, 30, 40, 50];

export default function RPPSelector({
    limit,
    handleChangeAction,
}: RPPSelectorProps): ReactElement {
    const [isOpen, setIsOpen] = useState(false);
    const wrapperRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        function handlePointerDown(event: PointerEvent): void {
            if (!wrapperRef.current?.contains(event.target as Node)) {
                setIsOpen(false);
            }
        }

        function handleKeyDown(event: KeyboardEvent): void {
            if (event.key === "Escape") {
                setIsOpen(false);
            }
        }

        document.addEventListener("pointerdown", handlePointerDown);
        document.addEventListener("keydown", handleKeyDown);

        return () => {
            document.removeEventListener("pointerdown", handlePointerDown);
            document.removeEventListener("keydown", handleKeyDown);
        };
    }, [isOpen]);

    function handleSelect(value: number): void {
        if (value !== limit) {
            handleChangeAction(value);
        }

        setIsOpen(false);
    }

    return (
        <div ref={wrapperRef} className={styles.wrapper}>
            <CustomButton
                leadingIcon={<ChevronsUpDown />}
                text={limit.toString()}
                aria-label="Select rows per page"
                aria-haspopup="listbox"
                aria-expanded={isOpen}
                textStyle={{ fontSize: "1.4rem", fontWeight: "700" }}
                onClick={() => setIsOpen((prev: boolean) => !prev)}
            />
            {isOpen && (
                <div
                    className={styles.popup}
                    role="listbox"
                    aria-label="Rows per page"
                >
                    {ROWS_PER_PAGE_OPTIONS.map((option: number) => (
                        <button
                            key={option}
                            type="button"
                            className={`${styles.option} ${
                                option === limit ? styles.selected : ""
                            }`}
                            role="option"
                            aria-selected={option === limit}
                            onClick={() => handleSelect(option)}
                        >
                            {option}
                        </button>
                    ))}
                </div>
            )}
        </div>
    );
}
