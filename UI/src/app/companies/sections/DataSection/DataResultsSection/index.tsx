"use client";

import { useRef } from "react";
import ResultsHeader from "./ResultsHeader";
import ResultsBody from "./ResultsBody";
import styles from "./styles.module.css";
import ResultsFooter from "./ResultsFooter";

interface CompanyData {
    companyName: string;
    address: string;
    phone: string;
    email: string;
    website: string;
    revenue: string;
    employees: string;
    companyStatus: string;
    sicCode: string;
}

export default function DataResultsSection() {
    const scrollContainerRef = useRef<HTMLDivElement>(null);

    const tempData: CompanyData[] = [
        {
            companyName: "Tech Innovators Inc",
            address: "101 Silicon Valley Way, San Jose, CA",
            phone: "555-0101",
            email: "contact@techinnovators.com",
            website: "www.techinnovators.com",
            revenue: "$50M",
            employees: "200",
            companyStatus: "Active",
            sicCode: "7371",
        },
        {
            companyName: "Global Logistics Solutions",
            address: "202 Harbor Dr, Miami, FL",
            phone: "555-0202",
            email: "info@globallogistics.com",
            website: "www.globallogistics.com",
            revenue: "$120M",
            employees: "500",
            companyStatus: "Active",
            sicCode: "4731",
        },
        {
            companyName: "EcoFriendly Energy",
            address: "303 Green St, Portland, OR",
            phone: "555-0303",
            email: "support@ecofriendly.com",
            website: "www.ecofriendly.com",
            revenue: "$30M",
            employees: "150",
            companyStatus: "Active",
            sicCode: "4911",
        },
        {
            companyName: "Creative Design Hub",
            address: "404 Art Ave, New York, NY",
            phone: "555-0404",
            email: "hello@creativedesign.com",
            website: "www.creativedesign.com",
            revenue: "$15M",
            employees: "80",
            companyStatus: "Active",
            sicCode: "7336",
        },
        {
            companyName: "Health Plus Services",
            address: "505 Wellness Blvd, Austin, TX",
            phone: "555-0505",
            email: "care@healthplus.com",
            website: "www.healthplus.com",
            revenue: "$80M",
            employees: "300",
            companyStatus: "Active",
            sicCode: "8099",
        },
        {
            companyName: "Smart Finance Group",
            address: "606 Wall St, Chicago, IL",
            phone: "555-0606",
            email: "advisor@smartfinance.com",
            website: "www.smartfinance.com",
            revenue: "$200M",
            employees: "450",
            companyStatus: "Active",
            sicCode: "6211",
        },
        {
            companyName: "Urban Realty Partners",
            address: "707 Skyline Rd, Seattle, WA",
            phone: "555-0707",
            email: "sales@urbanrealty.com",
            website: "www.urbanrealty.com",
            revenue: "$45M",
            employees: "120",
            companyStatus: "Active",
            sicCode: "6531",
        },
        {
            companyName: "Peak Performance Coaching",
            address: "808 Mountain View, Denver, CO",
            phone: "555-0808",
            email: "coach@peakperformance.com",
            website: "www.peakperformance.com",
            revenue: "$5M",
            employees: "25",
            companyStatus: "Active",
            sicCode: "8748",
        },
    ];

    return (
        <div className={styles.container}>
            <div className={styles.resultsWrapper} ref={scrollContainerRef}>
                <table className={styles.table}>
                    <ResultsHeader columns={Object.keys(tempData[0])} />
                    <ResultsBody data={tempData} />
                </table>
            </div>

            <ResultsFooter scrollContainerRef={scrollContainerRef} />
        </div>
    );
}
