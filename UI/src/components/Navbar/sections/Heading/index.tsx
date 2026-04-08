import type { ReactElement } from "react";
import Logo from "./components/Logo";
import styles from "./styles.module.css";

export default function Heading(): ReactElement {
    return (
        <div className={styles.container}>
            <Logo />
        </div>
    );
}
