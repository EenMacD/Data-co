import FilterLabel from "../../common/FilterLabel/FilterLabel";
import styles from "./FilterItem.module.css";
import CustomButton from "@/app/common/components/Buttons/CustomButton/CustomButton";

export default function FilterItem({
    label,
    text,
}: {
    label: string;
    text: string;
}) {
    return (
        <div className={styles.container}>
            <FilterLabel label={label} />
            <CustomButton text={text} />
        </div>
    );
}
