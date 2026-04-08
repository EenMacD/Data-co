import type { ReactElement } from "react";
import { Search } from "lucide-react";
import styles from "./styles.module.css";

interface SearchBarProps {
    placeholder: string;
    width?: string;
    height?: string;
}

export default function SearchBar({
    placeholder,
    width,
    height,
}: SearchBarProps): ReactElement {
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
