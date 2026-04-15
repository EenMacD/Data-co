import type { ReactElement } from "react";
import styles from "./page.module.css";
import TopBarSection from "./sections/TopBarSection";
import DataSection from "./sections/DataSection";

export const dynamic: string = "force-dynamic";

export default function Companies(): ReactElement {
    return (
        <main className={styles.main}>
            <TopBarSection />
            <DataSection />
        </main>
    );
}
