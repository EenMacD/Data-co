"use client";

import type { ReactElement, ReactNode } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import styles from "./styles.module.css";

interface NavLink {
    title: string;
    href: string;
    icon: ReactNode;
}

interface NavigationItemProps {
    navLink: NavLink;
}

export default function NavigationItem({
    navLink,
}: NavigationItemProps): ReactElement {
    const pathname: string = usePathname();
    const isActive: boolean = pathname === navLink.href;

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
