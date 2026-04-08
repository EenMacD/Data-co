import type { ReactElement } from "react";
import styles from "./styles.module.css";

interface NavSectionHeadingProps {
    title: string;
}

export default function NavSectionHeading({
    title,
}: NavSectionHeadingProps): ReactElement {
    return (
        <div className={styles.container}>
            <h3 className={styles.title}>{title}</h3>
            <div className={styles.line} />
        </div>
    );
}
