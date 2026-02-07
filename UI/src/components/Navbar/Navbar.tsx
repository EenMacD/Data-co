import Heading from "./sections/Heading/Heading";
import Navigation from "./sections/Navigation/Navigation";
import Utilities from "./sections/Utilities/Utilities";
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
