import FilterLabel from "../../common/FilterLabel";
import styles from "./styles.module.css";
import CustomButton from "@/app/common/components/Buttons/CustomButton";

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
