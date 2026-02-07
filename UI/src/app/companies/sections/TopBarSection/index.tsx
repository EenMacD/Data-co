import HeadingSection from "./HeadingSection";
import FilterSection from "./FiltersSection";
import styles from "./TopBarSection.module.css";

export default function TopBarSection() {
    return (
        <section className={styles.topBarSection}>
            <HeadingSection />
            <FilterSection />
        </section>
    );
}
