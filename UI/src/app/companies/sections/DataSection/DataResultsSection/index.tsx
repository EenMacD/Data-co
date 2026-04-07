import ScrollableTableArea from "./ScrollableTableArea";
import ResultsHeader from "./ResultsHeader";
import ResultsBody from "./ResultsBody";
import styles from "./styles.module.css";
import { searchCompanies } from "./services/fetch-companies";

export default async function DataResultsSection() {
    const response = await searchCompanies({
        limit: 20,
        offset: 1,
        orderBy: "company_number",
        includeTotal: false,
    });
    const companies = response.companies;

    const columns = [
        "Company Name",
        "Locality",
        "Number",
        "Status",
        "SIC Code",
        "Turnover",
        "Officers",
        "Date",
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
