import CustomButton from "@/app/common/components/CustomButton";
import { ChevronsUpDown } from "lucide-react";
import type { ReactElement } from "react";

type PageSelectorProps = {
    limit: number;
    handleChange: (value: number) => void;
};

export default function PageSelector({
    limit,
    handleChange,
}: PageSelectorProps): ReactElement {
    return (
        <CustomButton
            leadingIcon={<ChevronsUpDown />}
            text={limit.toString()}
            aria-label="Select rows per page"
            textStyle={{ fontSize: "1.4rem", fontWeight: "700" }}
            onClick={() => handleChange(5)}
        />
    );
}
