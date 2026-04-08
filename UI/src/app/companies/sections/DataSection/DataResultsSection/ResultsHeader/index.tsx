import type { ReactElement } from "react";
import styles from "./styles.module.css";

interface ResultsHeaderProps {
    columns: string[];
}

export default function ResultsHeader({
    columns,
}: ResultsHeaderProps): ReactElement {
    return (
        <thead className={styles.tableHeader}>
            <tr>
                {columns.map((column: string, index: number): ReactElement => (
                    <th key={index}>{column}</th>
                ))}
            </tr>
        </thead>
    );
}
