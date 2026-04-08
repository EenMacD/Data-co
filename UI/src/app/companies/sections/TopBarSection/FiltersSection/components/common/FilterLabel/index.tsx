import type { ReactElement } from "react";
import styles from "./styles.module.css";

interface FilterLabelProps {
    label: string;
}

export default function FilterLabel({
    label,
}: FilterLabelProps): ReactElement {
    return <h4 className={styles.label}>{label}</h4>;
}
