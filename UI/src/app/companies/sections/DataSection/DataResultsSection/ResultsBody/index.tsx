import type { ReactElement } from "react";
import { getString } from "@/app/common/helpers/get-string";
import Company from "../../../../models/company";
import styles from "./styles.module.css";
import TdContent from "./components/TdContent";
// import { formatCurrency } from "@/app/common/helpers/format-currency";

interface ResultsBodyProps {
    data: Company[];
    emptyMessage?: string;
}

export default function ResultsBody({
    data,
    emptyMessage = "No data available.",
}: ResultsBodyProps): ReactElement {
    return (
        <tbody className={styles.tableBody}>
            {data?.length ? (
                data.map(
                    (item: Company, index: number): ReactElement => (
                        <tr
                            key={index}
                            className={`${styles.tableRow} ${index % 2 === 0 ? styles.even : ""}`}
                        >
                            <TdContent>{item.company_name}</TdContent>
                            <TdContent>{item.company_number}</TdContent>
                            <TdContent>{getString(item.locality)}</TdContent>
                            <TdContent>{item.company_status}</TdContent>
                            <TdContent>{getString(item.sic_code)}</TdContent>
                            {/* <td>{getString(item.sic_code)}</td>
                    <td>{formatCurrency(item.turnover)}</td>
                    <td>{item.active_officers_count}</td>
                    <td>{item.incorporation_date}</td> */}
                        </tr>
                    ),
                )
            ) : (
                <tr className={styles.emptyRow}>
                    <td className={styles.emptyCell} colSpan={4}>
                        {emptyMessage}
                    </td>
                </tr>
            )}
        </tbody>
    );
}
