"use client";

import type { ReactElement } from "react";
import CustomButton from "@/app/common/components/CustomButton";
import FilterItem from "./components/FilterItem";
import styles from "./styles.module.css";
import { ListFilterPlus } from "lucide-react";
import type { FilterTemplateProps, PinnedFilter } from "./models/pinnedFilters";
import pinnedFilters from "./filters.config";
import { useCompanyFilters } from "./hooks/useCompanyFilters";
import RecursiveSelector from "./templates/RecursiveSelector/RecursiveSelector";
import MultiSelector from "./templates/MultiSelector/MultiSelector";

export default function Filters(): ReactElement {
    const { filters, applyStringArrayFilter } = useCompanyFilters();

    function renderFilterTemplate(
        filter: PinnedFilter,
        { onClose }: FilterTemplateProps,
    ): ReactElement {
        switch (filter.template) {
            case "Recursive":
                return (
                    <RecursiveSelector
                        data={filter.data}
                        selectedValues={filters[filter.id] ?? []}
                        searchPlaceholder={filter.searchPlaceholder}
                        getSubmitValue={filter.getSubmitValue}
                        disableFirstNodeSelection={
                            filter.disableFirstNodeSelection
                        }
                        submitOnOptionSelect={filter.submitOnOptionSelect}
                        onApply={(selectedValues: string[]) =>
                            applyStringArrayFilter(filter.id, selectedValues)
                        }
                        onClose={onClose}
                        cardStyles={filter.cardStyles}
                    />
                );
            case "MultiSelector":
                return (
                    <MultiSelector
                        data={filter.data}
                        selectedValues={filters[filter.id] ?? []}
                        searchPlaceholder={filter.searchPlaceholder}
                        getSubmitValue={filter.getSubmitValue}
                        submitOnOptionSelect={filter.submitOnOptionSelect}
                        onApply={(selectedValues: string[]) =>
                            applyStringArrayFilter(filter.id, selectedValues)
                        }
                        onClose={onClose}
                        cardStyles={filter.cardStyles}
                    />
                );
        }
    }

    return (
        <div className={styles.filtersWrapper}>
            {pinnedFilters.map(
                (filter: PinnedFilter): ReactElement => (
                    <FilterItem
                        key={filter.id}
                        filterData={filter}
                        renderTemplate={(props: FilterTemplateProps) =>
                            renderFilterTemplate(filter, props)
                        }
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
