import type { ChangeEventHandler, ReactElement } from "react";
import { Search } from "lucide-react";
import styles from "./styles.module.css";

interface SearchBarProps {
    placeholder: string;
    width?: string;
    height?: string;
    value?: string;
    onChange?: ChangeEventHandler<HTMLInputElement>;
}

export default function SearchBar({
    placeholder,
    width,
    height,
    value,
    onChange,
}: SearchBarProps): ReactElement {
    return (
        <div className={styles.container} style={{ width, height }}>
            <Search className={styles.icon} />
            <div className={styles.divider} />
            <input
                type="text"
                placeholder={placeholder}
                className={styles.input}
                value={value}
                onChange={onChange}
            />
        </div>
    );
}
