import Filters from "./components/Filters";
import ProfileSelector from "./components/ProfileSelector";
import styles from "./styles.module.css";

export default function FilterSection() {
    return (
        <section className={styles.container}>
            <ProfileSelector />
            <Filters />
        </section>
    );
}
