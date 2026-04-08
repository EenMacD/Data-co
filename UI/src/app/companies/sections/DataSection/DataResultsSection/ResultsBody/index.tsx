import styles from "./styles.module.css";

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

export default function ResultsBody({ data }: { data: CompanyData[] }) {
    return (
        <tbody className={styles.tableBody}>
            {data.map((item, index) => (
                <tr
                    key={index}
                    className={`${styles.tableRow} ${index % 2 === 0 ? styles.even : ""}`}
                >
                    <td>{item.companyName}</td>
                    <td>{item.address}</td>
                    <td>{item.phone}</td>
                    <td>{item.email}</td>
                    <td>{item.website}</td>
                    <td>{item.revenue}</td>
                    <td>{item.employees}</td>
                    <td>{item.companyStatus}</td>
                    <td>{item.sicCode}</td>
                </tr>
            ))}
        </tbody>
    );
}
