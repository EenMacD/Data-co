import React from 'react';
import styles from './Navbar.module.css';
import { Heart, User } from 'lucide-react';

const Navbar = () => {
    return (
        <nav className={styles.navbar}>
            <div className={styles.logo}>Zoopla</div>

            <div className={styles.navLinks}>
                <a href="/search" className={styles.link}>Companies</a>
                <a href="#" className={styles.link}>To rent</a>
                <a href="#" className={styles.link}>House prices</a>
                <a href="#" className={styles.link}>Agent valuation</a>
                <a href="#" className={styles.link}>Instant valuation</a>
                <a href="#" className={styles.link}>My Home</a>
            </div>

            <div className={styles.actions}>
                <a href="#" className={styles.actionLink}>
                    <Heart size={20} />
                    <span>Saved</span>
                </a>
                <button className={styles.signIn}>Sign in</button>
            </div>
        </nav>
    );
};

export default Navbar;
