import styles from "./page.module.css";
import TopBarSection from "./sections/TopBarSection/TopBarSection";
import DataSection from "./sections/DataSection/DataSection";

export default function Companies() {
    return (
        <main className={styles.main}>
            <TopBarSection />
            <DataSection />
        </main>
    );
}
