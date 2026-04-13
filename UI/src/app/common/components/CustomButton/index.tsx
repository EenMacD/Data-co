import type {
    ButtonHTMLAttributes,
    CSSProperties,
    ReactElement,
    ReactNode,
} from "react";
import styles from "./styles.module.css";

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
    trailingIcon?: ReactNode;
    leadingIcon?: ReactNode;
    variant?: "default" | "primary";
    shape?: "default" | "round";
    isRound?: boolean;
    isPrimary?: boolean;
    textStyle?: TextStyle;
    trailingIconStyle?: IconStyle;
    leadingIconStyle?: IconStyle;
    buttonStyle?: ButtonStyle;
    className?: string;
    style?: CSSProperties;
}

type NativeButtonProps = Omit<ButtonHTMLAttributes<HTMLButtonElement>, "style">;

type CustomButtonWithNativeProps = CustomButtonProps & NativeButtonProps;

export default function CustomButton({
    text,
    trailingIcon,
    leadingIcon,
    variant,
    shape,
    isRound,
    isPrimary,
    textStyle,
    trailingIconStyle,
    leadingIconStyle,
    buttonStyle,
    className = "",
    style,
    type = "button",
    ...buttonProps
}: CustomButtonWithNativeProps): ReactElement {
    const resolvedVariant: "default" | "primary" =
        variant ?? (isPrimary ? "primary" : "default");
    const resolvedShape: "default" | "round" =
        shape ?? (isRound ? "round" : "default");
    const hasText: boolean = Boolean(text);

    return (
        <button
            type={type}
            className={`${styles.container} ${
                resolvedShape === "round" ? styles.round : ""
            } ${resolvedVariant === "primary" ? styles.primary : ""} ${className}`}
            style={{
                background: buttonStyle?.background,
                padding: buttonStyle?.padding,
                ...style,
            }}
            {...buttonProps}
        >
            {leadingIcon && (
                <span
                    className={`${styles.icon} ${!hasText ? styles.iconOnly : ""}`}
                    style={{
                        width: leadingIconStyle?.size,
                        height: leadingIconStyle?.size,
                        color: leadingIconStyle?.color,
                    }}
                >
                    {leadingIcon}
                </span>
            )}
            {hasText && (
                <span
                    className={styles.text}
                    style={{
                        fontSize: textStyle?.fontSize,
                        color: textStyle?.color,
                        fontWeight: textStyle?.fontWeight,
                    }}
                >
                    {text}
                </span>
            )}
            {trailingIcon && (
                <span
                    className={`${styles.icon} ${!hasText ? styles.iconOnly : ""}`}
                    style={{
                        width: trailingIconStyle?.size,
                        height: trailingIconStyle?.size,
                        color: trailingIconStyle?.color,
                    }}
                >
                    {trailingIcon}
                </span>
            )}
        </button>
    );
}
