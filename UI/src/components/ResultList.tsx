import React from 'react';
import styles from './ResultList.module.css';
import CompanyCard from './CompanyCard';
import { Company, formatCurrency, getString } from '../services/api';

interface ResultListProps {
    companies: Company[];
    total: number;
    currentPage: number;
    pageSize: number;
    onPageChange: (newPage: number) => void;
    onPageSizeChange: (newPageSize: number) => void;
}

const ResultList: React.FC<ResultListProps> = ({
    companies,
    total,
    currentPage,
    pageSize,
    onPageChange,
    onPageSizeChange
}) => {
    if (companies.length === 0) {
        return (
            <div className={styles.listContainer}>
                <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
                    No companies found matching your search criteria.
                </div>
            </div>
        );
    }

    return (
        <div className={styles.listContainer}>
            <div className={styles.listHeader}>
                <span className={styles.resultCount}>
                    Showing {companies.length} of {total} companies
                </span>
                <div className={styles.sortControls}>
                    <span className={styles.sortLabel}>Sort by:</span>
                    <select className={styles.sortSelect}>
                        <option>Most Relevant</option>
                        <option>Revenue (High to Low)</option>
                        <option>Employees (High to Low)</option>
                        <option>Recently Updated</option>
                    </select>
                </div>
            </div>

            {companies.map((company) => (
                <CompanyCard
                    key={company.id}
                    name={company.company_name}
                    industry={getString(company.industry_category, 'N/A')}
                    location={`${getString(company.locality)}, ${getString(company.region)}`}
                    revenue={formatCurrency(company.turnover)}
                    employees={company.active_officers_count}
                    description={`Company Number: ${company.company_number} | Status: ${company.company_status}`}
                    lastUpdated={company.latest_accounts_date ? new Date(company.latest_accounts_date).toLocaleDateString() : 'N/A'}
                    status={company.company_status as 'Active' | 'Dissolved'}
                    valuation={company.net_worth ? formatCurrency(company.net_worth) : 'N/A'}
                />
            ))}

            {/* Pagination Controls */}
            <div className={styles.paginationContainer}>
                <div className={styles.paginationInfo}>
                    <span>
                        Showing {((currentPage - 1) * pageSize) + 1} to {Math.min(currentPage * pageSize, total)} of {total} results
                    </span>
                    <div className={styles.pageSizeSelector}>
                        <label htmlFor="pageSize">Results per page:</label>
                        <select
                            id="pageSize"
                            value={pageSize}
                            onChange={(e) => onPageSizeChange(Number(e.target.value))}
                            className={styles.pageSizeSelect}
                        >
                            <option value={10}>10</option>
                            <option value={25}>25</option>
                            <option value={50}>50</option>
                            <option value={100}>100</option>
                        </select>
                    </div>
                </div>

                <div className={styles.paginationControls}>
                    <button
                        onClick={() => onPageChange(1)}
                        disabled={currentPage === 1}
                        className={styles.pageButton}
                    >
                        First
                    </button>
                    <button
                        onClick={() => onPageChange(currentPage - 1)}
                        disabled={currentPage === 1}
                        className={styles.pageButton}
                    >
                        Previous
                    </button>

                    {/* Page numbers */}
                    <div className={styles.pageNumbers}>
                        {getPageNumbers(currentPage, Math.ceil(total / pageSize)).map((pageNum) => (
                            pageNum === -1 ? (
                                <span key={`ellipsis-${Math.random()}`} className={styles.ellipsis}>...</span>
                            ) : (
                                <button
                                    key={pageNum}
                                    onClick={() => onPageChange(pageNum)}
                                    className={`${styles.pageButton} ${pageNum === currentPage ? styles.activePage : ''}`}
                                >
                                    {pageNum}
                                </button>
                            )
                        ))}
                    </div>

                    <button
                        onClick={() => onPageChange(currentPage + 1)}
                        disabled={currentPage >= Math.ceil(total / pageSize)}
                        className={styles.pageButton}
                    >
                        Next
                    </button>
                    <button
                        onClick={() => onPageChange(Math.ceil(total / pageSize))}
                        disabled={currentPage >= Math.ceil(total / pageSize)}
                        className={styles.pageButton}
                    >
                        Last
                    </button>
                </div>
            </div>
        </div>
    );
};

// Helper function to generate page numbers with ellipsis
function getPageNumbers(currentPage: number, totalPages: number): number[] {
    const pages: number[] = [];
    const maxPagesToShow = 7;

    if (totalPages <= maxPagesToShow) {
        // Show all pages if total is less than max
        for (let i = 1; i <= totalPages; i++) {
            pages.push(i);
        }
    } else {
        // Always show first page
        pages.push(1);

        if (currentPage > 3) {
            pages.push(-1); // Ellipsis
        }

        // Show pages around current page
        const startPage = Math.max(2, currentPage - 1);
        const endPage = Math.min(totalPages - 1, currentPage + 1);

        for (let i = startPage; i <= endPage; i++) {
            pages.push(i);
        }

        if (currentPage < totalPages - 2) {
            pages.push(-1); // Ellipsis
        }

        // Always show last page
        pages.push(totalPages);
    }

    return pages;
}

export default ResultList;
