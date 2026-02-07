import Image from "next/image";
import styles from "./PageUtilProfileButton.module.css";

export default function PageUtilProfileButton() {
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
