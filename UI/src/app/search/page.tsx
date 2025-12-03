"use client";

import React, { useState, useEffect } from 'react';
import styles from './page.module.css';
import ResultList from '../../components/ResultList';
import { Filter } from 'lucide-react';
import { searchCompanies, type CompanySearchFilters, type SearchResponse } from '../../services/api';

export default function SearchPage() {
    const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
    const [appliedFiltersCount, setAppliedFiltersCount] = useState(1);

    // Filter states
    const [searchTerm, setSearchTerm] = useState('');
    const [industry, setIndustry] = useState('');
    const [location, setLocation] = useState('');
    const [revenue, setRevenue] = useState('');
    const [employees, setEmployees] = useState('');
    const [profitability, setProfitability] = useState('');
    const [companySize, setCompanySize] = useState('');
    const [companyAge, setCompanyAge] = useState('');
    const [companyStatus, setCompanyStatus] = useState('active');
    const [netAssets, setNetAssets] = useState('');
    const [debtLevel, setDebtLevel] = useState('');

    // Search results state
    const [searchResults, setSearchResults] = useState<SearchResponse | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Pagination state
    const [pageSize, setPageSize] = useState(25);
    const [currentPage, setCurrentPage] = useState(1);

    // Perform search
    const performSearch = async () => {
        setIsLoading(true);
        setError(null);

        try {
            const filters: CompanySearchFilters = {
                searchTerm: searchTerm || undefined,
                industry: industry || undefined,
                location: location || undefined,
                revenue: revenue || undefined,
                employees: employees || undefined,
                profitability: profitability || undefined,
                companySize: companySize || undefined,
                companyAge: companyAge || undefined,
                companyStatus: companyStatus || 'active',
                netAssets: netAssets || undefined,
                debtLevel: debtLevel || undefined,
                limit: pageSize,
                offset: (currentPage - 1) * pageSize,
            };

            const results = await searchCompanies(filters);
            setSearchResults(results);
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to search companies');
            console.error('Search error:', err);
        } finally {
            setIsLoading(false);
        }
    };

    // Handler for page changes
    const handlePageChange = (newPage: number) => {
        setCurrentPage(newPage);
    };

    // Handler for page size changes
    const handlePageSizeChange = (newPageSize: number) => {
        setPageSize(newPageSize);
        setCurrentPage(1); // Reset to first page when changing page size
    };

    // Search when pagination changes
    useEffect(() => {
        performSearch();
    }, [currentPage, pageSize]);

    // Reset to page 1 when filters change
    const performSearchWithReset = () => {
        setCurrentPage(1);
        if (currentPage === 1) {
            performSearch();
        }
    };

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
                    <input
                        type="text"
                        className={styles.filterSelect}
                        placeholder="Search by company name..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter') {
                                performSearchWithReset();
                            }
                        }}
                    />

                    <input
                        type="text"
                        className={styles.filterSelect}
                        placeholder="Location (e.g. Glasgow, London)..."
                        value={location}
                        onChange={(e) => setLocation(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter') {
                                performSearchWithReset();
                            }
                        }}
                    />

                    <select
                        className={styles.filterSelect}
                        value={industry}
                        onChange={(e) => setIndustry(e.target.value)}
                    >
                        <option value="">Industry</option>
                        <option value="tech">Technology</option>
                        <option value="finance">Finance</option>
                        <option value="retail">Retail</option>
                        <option value="manufacturing">Manufacturing</option>
                        <option value="professional">Professional Services</option>
                    </select>

                    <select
                        className={styles.filterSelect}
                        value={revenue}
                        onChange={(e) => setRevenue(e.target.value)}
                    >
                        <option value="">Revenue</option>
                        <option value="0-1m">Up to £1M</option>
                        <option value="1m-10m">£1M - £10M</option>
                        <option value="10m-50m">£10M - £50M</option>
                        <option value="50m+">£50M+</option>
                    </select>

                    <select
                        className={styles.filterSelect}
                        value={employees}
                        onChange={(e) => setEmployees(e.target.value)}
                    >
                        <option value="">Employees</option>
                        <option value="1-10">1-10</option>
                        <option value="11-50">11-50</option>
                        <option value="51-250">51-250</option>
                        <option value="251+">251+</option>
                    </select>

                    <select
                        className={styles.filterSelect}
                        value={profitability}
                        onChange={(e) => setProfitability(e.target.value)}
                    >
                        <option value="">Profitability</option>
                        <option value="profitable">Profitable</option>
                        <option value="loss_making">Loss Making</option>
                        <option value="breakeven">Break-even</option>
                    </select>

                    <button
                        className={styles.searchButton}
                        onClick={performSearchWithReset}
                        disabled={isLoading}
                    >
                        {isLoading ? 'Searching...' : 'Search'}
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
                                        <label>Industry</label>
                                        <select
                                            value={industry}
                                            onChange={(e) => setIndustry(e.target.value)}
                                        >
                                            <option value="">Select...</option>
                                            <option value="tech">Technology</option>
                                            <option value="finance">Finance</option>
                                            <option value="retail">Retail</option>
                                            <option value="manufacturing">Manufacturing</option>
                                            <option value="professional">Professional Services</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Location</label>
                                        <input
                                            type="text"
                                            placeholder="e.g. Glasgow, London..."
                                            value={location}
                                            onChange={(e) => setLocation(e.target.value)}
                                        />
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Employees</label>
                                        <select
                                            value={employees}
                                            onChange={(e) => setEmployees(e.target.value)}
                                        >
                                            <option value="">Select...</option>
                                            <option value="1-10">1-10</option>
                                            <option value="11-50">11-50</option>
                                            <option value="51-250">51-250</option>
                                            <option value="251+">251+</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Company Size</label>
                                        <select
                                            value={companySize}
                                            onChange={(e) => setCompanySize(e.target.value)}
                                        >
                                            <option value="">Select...</option>
                                            <option value="micro">Micro (1-10 employees)</option>
                                            <option value="small">Small (11-50 employees)</option>
                                            <option value="medium">Medium (51-250 employees)</option>
                                            <option value="large">Large (251+ employees)</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Company Age</label>
                                        <select
                                            value={companyAge}
                                            onChange={(e) => setCompanyAge(e.target.value)}
                                        >
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
                                        <select
                                            value={companyStatus}
                                            onChange={(e) => setCompanyStatus(e.target.value)}
                                        >
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
                                        <label>Revenue</label>
                                        <select
                                            value={revenue}
                                            onChange={(e) => setRevenue(e.target.value)}
                                        >
                                            <option value="">Select...</option>
                                            <option value="0-1m">Up to £1M</option>
                                            <option value="1m-10m">£1M - £10M</option>
                                            <option value="10m-50m">£10M - £50M</option>
                                            <option value="50m+">£50M+</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Profitability</label>
                                        <select
                                            value={profitability}
                                            onChange={(e) => setProfitability(e.target.value)}
                                        >
                                            <option value="">Select...</option>
                                            <option value="profitable">Profitable</option>
                                            <option value="loss_making">Loss Making</option>
                                            <option value="breakeven">Break-even</option>
                                        </select>
                                    </div>
                                    <div className={styles.filterGroup}>
                                        <label>Net Assets</label>
                                        <select
                                            value={netAssets}
                                            onChange={(e) => setNetAssets(e.target.value)}
                                        >
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
                                        <select
                                            value={debtLevel}
                                            onChange={(e) => setDebtLevel(e.target.value)}
                                        >
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
                                <button
                                    className={styles.clearButton}
                                    onClick={() => {
                                        setSearchTerm('');
                                        setIndustry('');
                                        setLocation('');
                                        setRevenue('');
                                        setEmployees('');
                                        setProfitability('');
                                        setCompanySize('');
                                        setCompanyAge('');
                                        setCompanyStatus('active');
                                        setNetAssets('');
                                        setDebtLevel('');
                                    }}
                                >
                                    Clear All
                                </button>
                                <button
                                    className={styles.applyButton}
                                    onClick={() => {
                                        setShowAdvancedFilters(false);
                                        performSearchWithReset();
                                    }}
                                    disabled={isLoading}
                                >
                                    Apply Filters
                                </button>
                            </div>
                        </div>
                    </div>
                </>
            )}

            <div className={styles.filterStatus}>
                <span className={styles.filterCount}>
                    {searchResults ? `${searchResults.total} companies found` : 'Loading...'}
                </span>
                <button
                    className={styles.clearAllLink}
                    onClick={() => {
                        setSearchTerm('');
                        setIndustry('');
                        setLocation('');
                        setRevenue('');
                        setEmployees('');
                        setProfitability('');
                        setCompanySize('');
                        setCompanyAge('');
                        setCompanyStatus('active');
                        setNetAssets('');
                        setDebtLevel('');
                        performSearchWithReset();
                    }}
                >
                    Clear all
                </button>
            </div>

            <main className={styles.mainContent}>
                {error && (
                    <div style={{ padding: '20px', color: 'red', textAlign: 'center' }}>
                        Error: {error}
                    </div>
                )}
                {isLoading && (
                    <div style={{ padding: '20px', textAlign: 'center' }}>
                        Loading companies...
                    </div>
                )}
                {searchResults && !isLoading && (
                    <ResultList
                        companies={searchResults.companies}
                        total={searchResults.total}
                        currentPage={currentPage}
                        pageSize={pageSize}
                        onPageChange={handlePageChange}
                        onPageSizeChange={handlePageSizeChange}
                    />
                )}
            </main>
        </div>
    );
}
