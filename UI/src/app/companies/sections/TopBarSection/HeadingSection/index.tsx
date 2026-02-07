import PageTitle from "./components/PageTitle";
import SearchBar from "../../../../common/components/SearchBar";
import PageUtil from "./components/PageUtil";
import styles from "./styles.module.css";

export default function HeadingSection() {
    return (
        <section className={styles.container}>
            <div className={styles.positionGroup}>
                <PageTitle />
                <SearchBar
                    placeholder="Search company by name, or ID..."
                    width="40rem"
                    height="5rem"
                />
            </div>
            <PageUtil />
        </section>
    );
}
