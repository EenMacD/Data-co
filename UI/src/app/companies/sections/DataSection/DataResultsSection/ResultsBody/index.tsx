import { getString } from "@/app/common/helpers/get-string";
import Company from "../models/company";
import styles from "./styles.module.css";
import { formatCurrency } from "@/app/common/helpers/format-currency";

export default function ResultsBody({ data }: { data: Company[] }) {
    return (
        <tbody className={styles.tableBody}>
            {data.map((item, index) => (
                <tr
                    key={index}
                    className={`${styles.tableRow} ${index % 2 === 0 ? styles.even : ""}`}
                >
                    <td>{item.company_name}</td>
                    <td>{getString(item.locality)}</td>
                    <td>{item.company_number}</td>
                    <td>{item.company_status}</td>
                    <td>{getString(item.primary_sic_code)}</td>
                    <td>{formatCurrency(item.turnover)}</td>
                    <td>{item.active_officers_count}</td>
                    <td>{item.incorporation_date}</td>
                </tr>
            ))}
        </tbody>
    );
}
