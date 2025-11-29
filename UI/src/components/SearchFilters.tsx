"use client";

import React, { useState } from 'react';
import styles from './SearchFilters.module.css';
import { ChevronDown, ChevronUp } from 'lucide-react';

const SearchFilters = () => {
    const [companyProfileOpen, setCompanyProfileOpen] = useState(true);
    const [financialHealthOpen, setFinancialHealthOpen] = useState(true);
    const [filtersVisible, setFiltersVisible] = useState(true);

    return (
        <div className={styles.sidebar}>
            <div className={styles.topBar}>
                <button
                    className={styles.advancedButton}
                    onClick={() => setFiltersVisible(!filtersVisible)}
                >
                    {filtersVisible ? 'Hide Filters' : 'Show Filters'}
                </button>
            </div>

            {filtersVisible && (
                <div className={styles.filtersPanel}>
                    <h3 className={styles.panelTitle}>Filters</h3>

                    {/* Company Profile Section */}
                    <div className={styles.filterSection}>
                        <button
                            className={styles.sectionHeader}
                            onClick={() => setCompanyProfileOpen(!companyProfileOpen)}
                        >
                            <span className={styles.sectionTitle}>Company Profile</span>
                            {companyProfileOpen ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
                        </button>

                        {companyProfileOpen && (
                            <div className={styles.sectionContent}>
                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Industry</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="tech">Technology</option>
                                        <option value="finance">Finance</option>
                                        <option value="retail">Retail</option>
                                        <option value="manufacturing">Manufacturing</option>
                                        <option value="professional">Professional Services</option>
                                    </select>
                                </div>

                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Company Size</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="micro">Micro (1-10 employees)</option>
                                        <option value="small">Small (11-50 employees)</option>
                                        <option value="medium">Medium (51-250 employees)</option>
                                        <option value="large">Large (251+ employees)</option>
                                    </select>
                                </div>

                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Company Age</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="0-2">0-2 years</option>
                                        <option value="3-5">3-5 years</option>
                                        <option value="6-10">6-10 years</option>
                                        <option value="11-20">11-20 years</option>
                                        <option value="21+">21+ years</option>
                                    </select>
                                </div>

                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Company Status</label>
                                    <select className={styles.select} defaultValue="active">
                                        <option value="active">Active only</option>
                                        <option value="all">All statuses</option>
                                        <option value="dissolved">Dissolved</option>
                                        <option value="liquidation">In liquidation</option>
                                    </select>
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Financial Health Section */}
                    <div className={styles.filterSection}>
                        <button
                            className={styles.sectionHeader}
                            onClick={() => setFinancialHealthOpen(!financialHealthOpen)}
                        >
                            <span className={styles.sectionTitle}>Financial Health</span>
                            {financialHealthOpen ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
                        </button>

                        {financialHealthOpen && (
                            <div className={styles.sectionContent}>
                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Profit</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="profitable">Profitable</option>
                                        <option value="loss_making">Loss Making</option>
                                        <option value="breakeven">Break-even</option>
                                    </select>
                                </div>

                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Net Assets</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="negative">Negative</option>
                                        <option value="0-100k">£0 - £100K</option>
                                        <option value="100k-1m">£100K - £1M</option>
                                        <option value="1m-10m">£1M - £10M</option>
                                        <option value="10m+">£10M+</option>
                                    </select>
                                </div>

                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Revenue Reported</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="0-1m">Up to £1M</option>
                                        <option value="1m-10m">£1M - £10M</option>
                                        <option value="10m-50m">£10M - £50M</option>
                                        <option value="50m-100m">£50M - £100M</option>
                                        <option value="100m+">£100M+</option>
                                    </select>
                                </div>

                                <div className={styles.filterGroup}>
                                    <label className={styles.label}>Debt Level</label>
                                    <select className={styles.select} defaultValue="">
                                        <option value="">Select...</option>
                                        <option value="none">No debt</option>
                                        <option value="low">Low (0-30% of assets)</option>
                                        <option value="medium">Medium (31-60% of assets)</option>
                                        <option value="high">High (61%+ of assets)</option>
                                    </select>
                                </div>
                            </div>
                        )}
                    </div>

                    <button className={styles.applyButton}>Apply filters</button>
                </div>
            )}
        </div>
    );
};

export default SearchFilters;
