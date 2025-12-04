"use client";

import React, { useState } from 'react';
import styles from './SearchBox.module.css';
import { Search } from 'lucide-react';

const SearchBox = () => {
    const [activeTab, setActiveTab] = useState<'sale' | 'rent' | 'house-prices'>('sale');

    return (
        <div className={styles.container}>
            <div className={styles.tabs}>
                <button
                    className={`${styles.tab} ${activeTab === 'sale' ? styles.activeTab : ''}`}
                    onClick={() => setActiveTab('sale')}
                >
                    For sale
                </button>
                <button
                    className={`${styles.tab} ${activeTab === 'rent' ? styles.activeTab : ''}`}
                    onClick={() => setActiveTab('rent')}
                >
                    To rent
                </button>
                <button
                    className={`${styles.tab} ${activeTab === 'house-prices' ? styles.activeTab : ''}`}
                    onClick={() => setActiveTab('house-prices')}
                >
                    House prices
                </button>
            </div>

            <div className={styles.searchForm}>
                <div className={styles.inputWrapper}>
                    <Search className={styles.searchIcon} size={20} />
                    <input
                        type="text"
                        placeholder="e.g. Oxford, NW3 or Waterloo Station"
                        className={styles.input}
                    />
                </div>
                <button className={styles.button}>Search</button>
            </div>
        </div>
    );
};

export default SearchBox;
