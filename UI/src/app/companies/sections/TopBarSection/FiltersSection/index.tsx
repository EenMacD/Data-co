import type { ReactElement } from "react";
import Filters from "./components/Filters";
import ProfileSelector from "./components/ProfileSelector";
import styles from "./styles.module.css";

export default function FilterSection(): ReactElement {
    return (
        <section className={styles.container}>
            <ProfileSelector />
            <Filters />
        </section>
    );
}
