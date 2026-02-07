import { Settings } from "lucide-react";
import styles from "./styles.module.css";

export default function PageUtilSettingsButton() {
    return (
        <div className={styles.container}>
            <Settings className={styles.icon} width={24} height={24} />
        </div>
    );
}
