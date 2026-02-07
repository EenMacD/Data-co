"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import styles from "./NavigationItem.module.css";

export default function NavigationItem({
    navLink,
}: {
    navLink: { title: string; href: string; icon: React.ReactNode };
}) {
    const pathname = usePathname();
    const isActive = pathname === navLink.href;

    return (
        <Link
            key={navLink.title}
            className={`${styles.navItem} ${isActive ? styles.active : ""}`}
            href={navLink.href}
        >
            <div className={styles.icon}>{navLink.icon}</div>
            <h3 className={styles.title}>{navLink.title}</h3>
        </Link>
    );
}
