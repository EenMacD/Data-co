import DataUtilSection from "./DataUtilSection";
import styles from "./styles.module.css";
import DataResultsSection from "./DataResultsSection";

import { searchCompanies } from "../../services/companies-api";
import { Company } from "../../models/company";

export default async function DataSection() {
    let companies: Company[] = [];
    try {
        const response = await searchCompanies({ limit: 5, offset: 0 });
        companies = response.companies;
    } catch (e) {
        console.error("Failed to fetch companies:", e);
    }

    console.log(companies);

    return (
        <section className={styles.dataSection}>
            <DataUtilSection />
            <DataResultsSection />
        </section>
    );
}
