import styles from "./FilterLabel.module.css";

export default function FilterLabel({ label }: { label: string }) {
    return <h4 className={styles.label}>{label}</h4>;
}
