import { Search } from "lucide-react";
import styles from "./styles.module.css";

export default function SearchBar({
    placeholder,
    width,
    height,
}: {
    placeholder: string;
    width?: string;
    height?: string;
}) {
    return (
        <div className={styles.container} style={{ width, height }}>
            <Search className={styles.icon} />
            <div className={styles.divider} />
            <input
                type="text"
                placeholder={placeholder}
                className={styles.input}
            />
        </div>
    );
}
