import styles from "./styles.module.css"; // "./CustomButton.module.css";

export default function CustomButton({
    text,
    trailingIcon,
    leadingIcon,
    isRound,
    isPrimary,
}: {
    text?: string;
    trailingIcon?: React.ReactNode;
    leadingIcon?: React.ReactNode;
    isRound?: boolean;
    isPrimary?: boolean;
}) {
    return (
        <div
            className={`${styles.container} ${isRound ? styles.round : ""} ${
                isPrimary ? styles.primary : ""
            }`}
        >
            {leadingIcon && <div className={styles.icon}>{leadingIcon}</div>}
            {text && <p className={styles.text}>{text}</p>}
            {trailingIcon && <div className={styles.icon}>{trailingIcon}</div>}
        </div>
    );
}
