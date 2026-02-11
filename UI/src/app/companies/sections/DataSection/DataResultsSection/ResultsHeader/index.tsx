import styles from "./styles.module.css";

export default function ResultsHeader({ columns }: { columns: string[] }) {
    return (
        <thead className={styles.tableHeader}>
            <tr>
                {columns.map((column, index) => (
                    <th key={index}>{column}</th>
                ))}
            </tr>
        </thead>
    );
}
