import CustomButton from "@/app/common/components/Buttons/CustomButton";
import FilterItem from "./FilterItem";
import styles from "./styles.module.css";
import { ListFilterPlus } from "lucide-react";

export default function Filters() {
    const pinnedFilters: { label: string; text: string }[] = [
        { label: "Location", text: "Select Option" },
        { label: "SIC Code", text: "Select Option" },
        { label: "Company Status", text: "Select Option" },
        { label: "Revenue", text: "Select Option" },
    ];

    return (
        <div className={styles.container}>
            {pinnedFilters.map((filter, index) => (
                <FilterItem
                    key={index}
                    label={filter.label}
                    text={filter.text}
                />
            ))}
            <CustomButton
                text="Filters"
                leadingIcon={<ListFilterPlus />}
                isPrimary
            />
        </div>
    );
}
