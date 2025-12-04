import React from 'react';
import styles from './CompanyCard.module.css';
import { Building2, MapPin, Users, TrendingUp } from 'lucide-react';

interface CompanyCardProps {
    name: string;
    industry: string;
    location: string;
    revenue: string;
    employees: number;
    description: string;
    lastUpdated: string;
    status: 'Active' | 'Dissolved' | 'Liquidation';
    valuation: string;
}

const CompanyCard: React.FC<CompanyCardProps> = ({
    name,
    industry,
    location,
    revenue,
    employees,
    description,
    lastUpdated,
    status,
    valuation
}) => {
    return (
        <div className={styles.card}>
            <div className={styles.imageContainer}>
                <div className={styles.placeholderImage}>
                    <Building2 size={48} color="#aaa" />
                </div>
            </div>
            <div className={styles.content}>
                <div className={styles.header}>
                    <div>
                        <h3 className={styles.companyName}>{name}</h3>
                        <div className={styles.industry}>
                            {industry} • {location}
                        </div>
                    </div>
                    <div className={styles.value}>
                        {valuation}
                        <span className={styles.valueLabel}>Est. Valuation</span>
                    </div>
                </div>

                <div className={styles.detailsGrid}>
                    <div className={styles.detailItem}>
                        <span className={styles.detailLabel}>Revenue</span>
                        <span className={styles.detailValue}>{revenue}</span>
                    </div>
                    <div className={styles.detailItem}>
                        <span className={styles.detailLabel}>Employees</span>
                        <span className={styles.detailValue}>{employees}</span>
                    </div>
                    <div className={styles.detailItem}>
                        <span className={styles.detailLabel}>Growth</span>
                        <span className={styles.detailValue} style={{ color: '#1e7e34' }}>+12%</span>
                    </div>
                </div>

                <p className={styles.description}>{description}</p>

                <div className={styles.footer}>
                    <div className={styles.updateTime}>
                        Updated: {lastUpdated}
                    </div>
                    <span className={`${styles.status} ${status === 'Active' ? styles.statusActive : ''}`}>
                        {status}
                    </span>
                </div>
            </div>
        </div>
    );
};

export default CompanyCard;
