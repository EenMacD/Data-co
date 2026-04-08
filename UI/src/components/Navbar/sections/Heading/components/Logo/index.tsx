import type { ReactElement } from "react";
import styles from "./styles.module.css"; // './Logo.module.css'

export default function Logo(): ReactElement {
    return (
        <div className={styles.container}>
            <span className={styles.logo}>Q</span>
            <h1 className={styles.title}>Quantra</h1>
        </div>
    );
}
