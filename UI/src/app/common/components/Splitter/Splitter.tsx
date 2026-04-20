import { ReactElement } from "react";
import styles from "./styles.module.css";

type SplitterProps = {
    isVertical?: boolean;
};

export default function Splitter({ isVertical }: SplitterProps): ReactElement {
    return (
        <div className={`${isVertical ? styles.vertical : ''} ${styles.line}`} />
    );
}
