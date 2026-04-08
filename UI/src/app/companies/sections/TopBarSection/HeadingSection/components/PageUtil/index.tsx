import styles from "./styles.module.css";
import PageUtilSettingsButton from "./components/PageUtilSettingsButton";
import PageUtilProfileButton from "./components/PageUtilProfileButton";

export default function PageUtil() {
    return (
        <div className={styles.container}>
            <PageUtilSettingsButton />
            <PageUtilProfileButton />
        </div>
    );
}
