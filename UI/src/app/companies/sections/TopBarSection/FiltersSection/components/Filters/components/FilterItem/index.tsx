import { useEffect, useRef, useState, type ReactElement } from "react";
import FilterLabel from "../../../common/FilterLabel";
import styles from "./styles.module.css";
import CustomButton from "@/app/common/components/CustomButton";
import type {
    FilterTemplateRenderer,
    PinnedFilter,
} from "../../models/pinnedFilters";

export default function FilterItem({
    filterData,
    renderTemplate,
}: {
    filterData: PinnedFilter;
    renderTemplate: FilterTemplateRenderer;
}): ReactElement {
    const [isOpen, setIsOpen] = useState(false);
    const wrapperRef = useRef<HTMLDivElement>(null);

    const { label, text } = filterData;

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        function handlePointerDown(event: PointerEvent): void {
            if (!wrapperRef.current?.contains(event.target as Node)) {
                setIsOpen(false);
            }
        }

        document.addEventListener("pointerdown", handlePointerDown);

        return () => {
            document.removeEventListener("pointerdown", handlePointerDown);
        };
    }, [isOpen]);

    function handleClose(): void {
        setIsOpen(false);
    }

    return (
        <div ref={wrapperRef} className={styles.wrapper}>
            <FilterLabel label={label} />
            <CustomButton
                text={text}
                onClick={() => setIsOpen((prev: boolean) => !prev)}
            />
            {isOpen && (
                <div className={styles.popup}>
                    {renderTemplate({ onClose: handleClose })}
                </div>
            )}
        </div>
    );
}
