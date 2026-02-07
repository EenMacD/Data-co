import Heading from "./sections/Heading";
import Navigation from "./sections/Navigation";
import Utilities from "./sections/Utilities";
import styles from "./Navbar.module.css";

export default function Navbar() {
    return (
        <section className={styles.navbar}>
            <Heading />
            <Navigation />
            <Utilities />
        </section>
    );
}
