import type { ReactElement } from "react";
import Heading from "./sections/Heading";
import Navigation from "./sections/Navigation";
import Utilities from "./sections/Utilities";
import styles from "./styles.module.css";

export default function Navbar(): ReactElement {
    return (
        <section className={styles.navbar}>
            <Heading />
            <Navigation />
            <Utilities />
        </section>
    );
}
