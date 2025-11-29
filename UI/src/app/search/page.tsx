"use client";

import React, { useState } from 'react';
import styles from './page.module.css';
import ResultList from '../../components/ResultList';
import { Filter } from 'lucide-react';

export default function SearchPage() {
    const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
    const [appliedFiltersCount, setAppliedFiltersCount] = useState(1);

    return (
        <div className={styles.pageContainer}>
            <div className={styles.searchHeader}>
                <h1 className={styles.searchTitle}>UK Companies Search</h1>
                <button
                    className={styles.advancedFiltersButton}
                    onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
                >
                    <Filter size={16} />
                    Advanced Filters
                    {appliedFiltersCount > 0 && (
                        <span className={styles.filterBadge}>{appliedFiltersCount}</span>
                    )}
                </button>
            </div>

            <div className={styles.filtersBarWrapper}>
                <div className={styles.filtersBar}>
                    <select className={styles.filterSelect}>
                        <option value="">Industry</option>
                        <option value="">Any Industry</option>
                        <option value="tech">Technology</option>
                        <option value="finance">Finance</option>
                        <option value="retail">Retail</option>
                        <option value="manufacturing">Manufacturing</option>
                        <option value="professional">Professional Services</option>
                    </select>

                    <select className={styles.filterSelect}>
                        <option value="">Location</option>
                        <option value="">All locations</option>
                        <option value="london">London</option>
                        <option value="manchester">Manchester</option>
                        <option value="birmingham">Birmingham</option>
                        <option value="edinburgh">Edinburgh</option>
                        <option value="bristol">Bristol</option>
                    </select>

                    <select className={styles.filterSelect}>
                        <option value="">Revenue</option>
                        <option value="">Any</option>
                        <option value="0-1m">Up to £1M</option>
                        <option value="1m-10m">£1M - £10M</option>
                        <option value="10m-50m">£10M - £50M</option>
                        <option value="50m+">£50M+</option>
                    </select>

                    <select className={styles.filterSelect}>
                        <option value="">Employees</option>
                        <option value="">Any</option>
                        <option value="1-10">1-10</option>
                        <option value="11-50">11-50</option>
                        <option value="51-250">51-250</option>
                        <option value="251+">251+</option>
                    </select>

                    <select className={styles.filterSelect}>
                        <option value="">Profitability</option>
                        <option value="">Any</option>
                        <option value="profitable">Profitable</option>
                        <option value="loss_making">Loss Making</option>
                        <option value="breakeven">Break-even</option>
                    </select>

                    <button className={styles.searchButton}>
                        Search
                    </button>
                </div>
            </div>

            {showAdvancedFilters && (
                <>
                    <div
                        className={styles.backdrop}
                        onClick={() => setShowAdvancedFilters(false)}
                    />
                    <div className={styles.advancedFiltersPanel}>
                        <div className={styles.advancedFiltersContent}>
                            <div className={styles.panelHeader}>
                                <h3 className={styles.panelTitle}>Advanced Filters</h3>
                                <button
                                    className={styles.closeButton}
                                    onClick={() => setShowAdvancedFilters(false)}
                                    aria-label="Close filters"
                                >
                                    ×
                                </button>
                            </div>

                            <div className={styles.filterSection}>
                                <h4 className={styles.sectionTitle}>Company Profile</h4>
                                <div className={styles.filterGrid}>
                                    <div className={styles.filterGroup}>
                                        <label>Company Size</label>
                                        <select>
                                            <option value="">Select...</option>
                                            <option value="micro">Micro (1-10 employees)</option>
                                            <option value="small">Small (11-50 employees)</option>
                                            <option value="medium">Medium (51-250 employees)</option>
                                            <option value="large">Large (251+ employees)</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Company Age</label>
                                        <select>
                                            <option value="">Select...</option>
                                            <option value="0-2">0-2 years</option>
                                            <option value="3-5">3-5 years</option>
                                            <option value="6-10">6-10 years</option>
                                            <option value="11-20">11-20 years</option>
                                            <option value="21+">21+ years</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Company Status</label>
                                        <select defaultValue="active">
                                            <option value="active">Active only</option>
                                            <option value="all">All statuses</option>
                                            <option value="dissolved">Dissolved</option>
                                            <option value="liquidation">In liquidation</option>
                                        </select>
                                    </div>
                                </div>
                            </div>

                            <div className={styles.filterSection}>
                                <h4 className={styles.sectionTitle}>Financial Health</h4>
                                <div className={styles.filterGrid}>
                                    <div className={styles.filterGroup}>
                                        <label>Net Assets</label>
                                        <select>
                                            <option value="">Select...</option>
                                            <option value="negative">Negative</option>
                                            <option value="0-100k">£0 - £100K</option>
                                            <option value="100k-1m">£100K - £1M</option>
                                            <option value="1m-10m">£1M - £10M</option>
                                            <option value="10m+">£10M+</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Debt Level</label>
                                        <select>
                                            <option value="">Select...</option>
                                            <option value="none">No debt</option>
                                            <option value="low">Low (0-30% of assets)</option>
                                            <option value="medium">Medium (31-60% of assets)</option>
                                            <option value="high">High (61%+ of assets)</option>
                                        </select>
                                    </div>
                                </div>
                            </div>

                            <div className={styles.filterActions}>
                                <button className={styles.clearButton}>Clear All</button>
                                <button className={styles.applyButton}>Apply Filters</button>
                            </div>
                        </div>
                    </div>
                </>
            )}

            <div className={styles.filterStatus}>
                <span className={styles.filterCount}>{appliedFiltersCount} filter applied</span>
                <button className={styles.clearAllLink}>Clear all</button>
            </div>

            <main className={styles.mainContent}>
                <ResultList />
            </main>
        </div>
    );
}
