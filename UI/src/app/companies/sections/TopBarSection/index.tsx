import type { ReactElement } from "react";
import HeadingSection from "./HeadingSection";
import FilterSection from "./FiltersSection";
import styles from "./styles.module.css";

export default function TopBarSection(): ReactElement {
    return (
        <section className={styles.topBarSection}>
            <HeadingSection />
            <FilterSection />
        </section>
    );
}
