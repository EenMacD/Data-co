import { Albert_Sans, Inter } from "next/font/google";

export const inter = Inter({
    subsets: ["latin"],
    variable: "--font-inter",
    display: "swap",
});

export const albert_sans = Albert_Sans({
    subsets: ["latin"],
    variable: "--font-albert_sans",
    display: "swap",
});

const interVariable: string = (inter as { variable: string }).variable;
const albertSansVariable: string = (albert_sans as { variable: string }).variable;

export const fontsClassNames: string =
    `${interVariable} ${albertSansVariable}`;
