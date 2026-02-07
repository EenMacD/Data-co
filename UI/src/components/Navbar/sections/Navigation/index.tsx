import styles from "./styles.module.css";
import NavigationItem from "./NavigationItem";
import NavSectionHeading from "../../components/NavSectionHeading";
import { House, Building2, Users } from "lucide-react";

export default function Navigation() {
    const navItems = [
        { icon: <House />, title: "Dashboard", href: "/" },
        { icon: <Building2 />, title: "Companies", href: "/companies" },
        { icon: <Users />, title: "PSCs", href: "/pscs" },
    ];

    return (
        <>
            <NavSectionHeading title="Navigation" />
            <ul className={styles.navList}>
                {navItems.map((navLink) => (
                    <NavigationItem key={navLink.title} navLink={navLink} />
                ))}
            </ul>
        </>
    );
}
