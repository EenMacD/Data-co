"use client";

import type { ReactElement } from "react";
import CustomButton from "@/app/common/components/CustomButton";
import FilterItem from "./components/FilterItem";
import styles from "./styles.module.css";
import { ListFilterPlus } from "lucide-react";
import PinnedFilter from "./models/pinnedFilters";
import localityData from "./data/locality.json";
import sicData from "./data/sic.json";

// templates
import MultiSelector from "./templates/RecursiveSelector/RecursiveSelector";

export default function Filters(): ReactElement {
    const pinnedFilters: PinnedFilter[] = [
        {
            id: "locations",
            label: "Location",
            text: "Select Location",
            template: (
                <MultiSelector filterId="locations" data={localityData} />
            ),
        },
        {
            id: "industry",
            label: "Industry",
            text: "Select industry",
            template: (
                <MultiSelector
                    filterId="industry"
                    data={sicData}
                    searchPlaceholder="Search industry or SIC code"
                    disableFirstNodeSelection
                />
            ),
        },
        // { label: "Company Status", text: "Select Option" },
        // { label: "Revenue", text: "Select Option" },
    ];

    return (
        <div className={styles.filtersWrapper}>
            {pinnedFilters.map(
                (filter: PinnedFilter): ReactElement => (
                    <FilterItem key={filter.id} filterData={filter} />
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
