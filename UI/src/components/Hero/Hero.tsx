import React from 'react';
import styles from './Hero.module.css';
import SearchBox from '../Search/SearchBox';

const Hero = () => {
    return (
        <section className={styles.hero}>
            <h1 className={styles.title}>We know what a home is really worth</h1>
            <p className={styles.subtitle}>Find homes for sale, for rent, or check house prices with the UK's most comprehensive property website.</p>
            <SearchBox />
        </section>
    );
};

export default Hero;
