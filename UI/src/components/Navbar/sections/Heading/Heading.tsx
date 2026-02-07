import Logo from "./components/Logo/Logo";
import styles from "./Heading.module.css";

export default function Heading() {
    return (
        <div className={styles.container}>
            <Logo />
        </div>
    );
}
