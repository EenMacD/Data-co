"use client";

import type { ReactElement } from "react";
import CustomButton from "@/app/common/components/Buttons/CustomButton";
import FilterItem from "./components/FilterItem";
import styles from "./styles.module.css";
import { ListFilterPlus } from "lucide-react";
import PinnedFilter from "./models/pinnedFilters";

// templates
import MultiSelector from "./templates/RecursiveSelector/RecursiveSelector";

export default function Filters(): ReactElement {
    const pinnedFilters: PinnedFilter[] = [
        {
            label: "Location",
            text: "Select Location",
            template: <MultiSelector />,
        },
        // { label: "SIC Code", text: "Select Option" },
        // { label: "Company Status", text: "Select Option" },
        // { label: "Revenue", text: "Select Option" },
    ];

    return (
        <div className={styles.container}>
            {pinnedFilters.map(
                (filter: PinnedFilter, index: number): ReactElement => (
                    <FilterItem key={index} filterData={filter} />
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
