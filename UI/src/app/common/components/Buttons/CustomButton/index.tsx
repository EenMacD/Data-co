import styles from "./styles.module.css"; // "./CustomButton.module.css";

interface TextStyle {
    fontSize?: string;
    color?: string;
    fontWeight?: string | number;
}

interface IconStyle {
    size?: string;
    color?: string;
}

interface ButtonStyle {
    padding?: string;
    background?: string;
}

interface CustomButtonProps {
    text?: string;
    trailingIcon?: React.ReactNode;
    leadingIcon?: React.ReactNode;
    isRound?: boolean;
    isPrimary?: boolean;
    textStyle?: TextStyle;
    trailingIconStyle?: IconStyle;
    leadingIconStyle?: IconStyle;
    buttonStyle?: ButtonStyle;
}

export default function CustomButton({
    text,
    trailingIcon,
    leadingIcon,
    isRound,
    isPrimary,
    textStyle,
    trailingIconStyle,
    leadingIconStyle,
    buttonStyle,
}: CustomButtonProps) {
    return (
        <div
            className={`${styles.container} ${isRound ? styles.round : ""} ${
                isPrimary ? styles.primary : ""
            }`}
            style={{
                background: buttonStyle?.background,
                padding: buttonStyle?.padding,
            }}
        >
            {leadingIcon && (
                <div
                    className={`${styles.icon} ${!text ? styles.iconOnly : ""}`}
                    style={{
                        width: leadingIconStyle?.size,
                        height: leadingIconStyle?.size,
                        color: leadingIconStyle?.color,
                    }}
                >
                    {leadingIcon}
                </div>
            )}
            {text && (
                <p
                    className={styles.text}
                    style={{
                        fontSize: textStyle?.fontSize,
                        color: textStyle?.color,
                        fontWeight: textStyle?.fontWeight,
                    }}
                >
                    {text}
                </p>
            )}
            {trailingIcon && (
                <div
                    className={`${styles.icon} ${!text ? styles.iconOnly : ""}`}
                    style={{
                        width: trailingIconStyle?.size,
                        height: trailingIconStyle?.size,
                        color: trailingIconStyle?.color,
                    }}
                >
                    {trailingIcon}
                </div>
            )}
        </div>
    );
}
