import styles from "./PageTitle.module.css";
import { Building2 } from "lucide-react";

export default function PageTitle() {
    return (
        <div className={styles.container}>
            <Building2 className={styles.icon} />
            <h1 className={styles.title}>Companies</h1>
        </div>
    );
}
