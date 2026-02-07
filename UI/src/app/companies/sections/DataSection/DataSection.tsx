import DataUtilSection from "./DataUtilSection/DataUtilSection";
import styles from "./DataSection.module.css";

export default function DataSection() {
    return (
        <section className={styles.dataSection}>
            <DataUtilSection />
        </section>
    );
}
