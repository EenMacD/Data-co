import HeadingSection from "./HeadingSection/HeadingSection";
import FilterSection from "./FiltersSection/FilterSection";
import styles from "./TopBarSection.module.css";

export default function TopBarSection() {
    return (
        <section className={styles.topBarSection}>
            <HeadingSection />
            <FilterSection />
        </section>
    );
}
