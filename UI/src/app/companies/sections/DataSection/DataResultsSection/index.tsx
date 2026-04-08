import type { ReactElement } from "react";
import ScrollableTableArea from "./ScrollableTableArea";
import ResultsHeader from "./ResultsHeader";
import ResultsBody from "./ResultsBody";
import styles from "./styles.module.css";
import { searchCompanies } from "./services/fetch-companies";
import { SearchResponse } from "./models/search-response";
import Company from "./models/company";

export default async function DataResultsSection(): Promise<ReactElement> {
    const response: SearchResponse = await searchCompanies({
        limit: 20,
        offset: 0,
        orderBy: "company_number",
        includeTotal: false,
    });
    const companies: Company[] = response.companies;

    console.log(companies);
    //TODO: now that you got a data response start working on the filters one by one and insure accuracy of the data

    const columns: string[] = [
        "Company Name",
        "Company Number",
        "Locality",
        "Status",
        // "SIC Code",
        // "Turnover",
        // "Officers",
        // "Date",
    ];

    return (
        <div className={styles.container}>
            <ScrollableTableArea>
                <table className={styles.table}>
                    <ResultsHeader columns={columns} />
                    <ResultsBody data={companies} />
                </table>
            </ScrollableTableArea>
        </div>
    );
}
