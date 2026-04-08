import type { ReactElement } from "react";
import Image from "next/image";
import styles from "./styles.module.css";

export default function PageUtilProfileButton(): ReactElement {
    return (
        <div>
            <Image
                className={styles.image}
                src="/images/util/handsome-iain.jpeg"
                alt="Handsome Iain"
                width={50}
                height={50}
            />
        </div>
    );
}
