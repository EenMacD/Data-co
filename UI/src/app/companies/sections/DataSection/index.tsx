import DataUtilSection from "./DataUtilSection";
import styles from "./styles.module.css";
import DataResultsSection from "./DataResultsSection";

export default function DataSection() {
    return (
        <section className={styles.dataSection}>
            <DataUtilSection />
            <DataResultsSection />
        </section>
    );
}
