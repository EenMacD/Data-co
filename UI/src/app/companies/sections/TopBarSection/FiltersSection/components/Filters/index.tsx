import type { ReactElement } from "react";
import CustomButton from "@/app/common/components/Buttons/CustomButton";
import FilterItem from "./FilterItem";
import styles from "./styles.module.css";
import { ListFilterPlus } from "lucide-react";

interface PinnedFilter {
    label: string;
    text: string;
}

export default function Filters(): ReactElement {
    const pinnedFilters: PinnedFilter[] = [
        { label: "Location", text: "Select Location" }, //TODO: you are creating the location filter
        // { label: "SIC Code", text: "Select Option" },
        // { label: "Company Status", text: "Select Option" },
        // { label: "Revenue", text: "Select Option" },
    ];

    return (
        <div className={styles.container}>
            {pinnedFilters.map(
                (filter: PinnedFilter, index: number): ReactElement => (
                    <FilterItem
                        key={index}
                        label={filter.label}
                        text={filter.text}
                    />
                ),
            )}
            <CustomButton
                text="Filters"
                leadingIcon={<ListFilterPlus />}
                isPrimary
            />
        </div>
    );
}
