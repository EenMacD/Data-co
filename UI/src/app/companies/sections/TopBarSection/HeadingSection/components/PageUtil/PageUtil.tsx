import styles from "./PageUtil.module.css";
import PageUtilSettingsButton from "./components/PageUtilSettingsButton/PageUtilSettingsButton";
import PageUtilProfileButton from "./components/PageUtilProfileButton/PageUtilProfileButton";

export default function PageUtil() {
    return (
        <div className={styles.container}>
            <PageUtilSettingsButton />
            <PageUtilProfileButton />
        </div>
    );
}
