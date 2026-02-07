import styles from "./styles.module.css";

export default function NavSectionHeading({ title }: { title: string }) {
    return (
        <div className={styles.container}>
            <h3 className={styles.title}>{title}</h3>
            <div className={styles.line} />
        </div>
    );
}
