import { Albert_Sans, Inter } from "next/font/google";

export const inter: ReturnType<typeof Inter> = Inter({
    subsets: ["latin"],
    variable: "--font-inter",
    display: "swap",
});

export const albert_sans: ReturnType<typeof Albert_Sans> = Albert_Sans({
    subsets: ["latin"],
    variable: "--font-albert_sans",
    display: "swap",
});

export const fontsClassNames: string =
    `${inter.variable} ${albert_sans.variable}`;
