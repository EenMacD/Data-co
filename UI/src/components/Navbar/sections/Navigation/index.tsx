import type { ReactElement } from "react";
import styles from "./styles.module.css";
import NavigationItem from "./NavigationItem";
import NavSectionHeading from "../../components/NavSectionHeading";
import { House, Building2, Users } from "lucide-react";

interface NavItem {
    icon: ReactElement;
    title: string;
    href: string;
}

export default function Navigation(): ReactElement {
    const navItems: NavItem[] = [
        { icon: <House />, title: "Dashboard", href: "/" },
        { icon: <Building2 />, title: "Companies", href: "/companies" },
        { icon: <Users />, title: "PSCs", href: "/pscs" },
    ];

    return (
        <>
            <NavSectionHeading title="Navigation" />
            <ul className={styles.navList}>
                {navItems.map(
                    (navLink: NavItem): ReactElement => (
                        <NavigationItem key={navLink.title} navLink={navLink} />
                    ),
                )}
            </ul>
        </>
    );
}
