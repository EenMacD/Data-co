import React from 'react';
import styles from './ResultList.module.css';
import CompanyCard from './CompanyCard';

const MOCK_DATA = [
    {
        id: 1,
        name: "Bristol Finance Solutions Ltd",
        industry: "Professional Services",
        location: "Bristol, South West",
        revenue: "£2.1M",
        employees: 62,
        description: "Leading provider of financial consulting services for small to medium enterprises in the South West region. Specializing in tax planning and business growth strategies.",
        lastUpdated: "2 days ago",
        status: "Active" as const,
        valuation: "£4.5M - £6M"
    },
    {
        id: 2,
        name: "TechFlow Dynamics",
        industry: "Technology",
        location: "London, Shoreditch",
        revenue: "£15.4M",
        employees: 145,
        description: "Innovative software development company focusing on AI-driven workflow automation tools for enterprise clients. Rapidly growing with a strong client base in the fintech sector.",
        lastUpdated: "4 hours ago",
        status: "Active" as const,
        valuation: "£45M - £60M"
    },
    {
        id: 3,
        name: "GreenLeaf Logistics",
        industry: "Transportation",
        location: "Manchester",
        revenue: "£8.2M",
        employees: 85,
        description: "Sustainable logistics and supply chain management solutions. Committed to reducing carbon footprint through electric vehicle fleets and optimized routing algorithms.",
        lastUpdated: "1 week ago",
        status: "Active" as const,
        valuation: "£12M - £15M"
    },
    {
        id: 4,
        name: "Northern Manufacturing Co",
        industry: "Manufacturing",
        location: "Leeds",
        revenue: "£4.5M",
        employees: 42,
        description: "Precision engineering and manufacturing of automotive components. Established in 1985 with a reputation for quality and reliability.",
        lastUpdated: "3 days ago",
        status: "Active" as const,
        valuation: "£6M - £8M"
    }
];

const ResultList = () => {
    return (
        <div className={styles.listContainer}>
            <div className={styles.listHeader}>
                <span className={styles.resultCount}>Showing 21 companies</span>
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

            {MOCK_DATA.map((company) => (
                <CompanyCard
                    key={company.id}
                    name={company.name}
                    industry={company.industry}
                    location={company.location}
                    revenue={company.revenue}
                    employees={company.employees}
                    description={company.description}
                    lastUpdated={company.lastUpdated}
                    status={company.status}
                    valuation={company.valuation}
                />
            ))}
        </div>
    );
};

export default ResultList;
