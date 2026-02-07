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
})

export const fontsClassNames = `${inter.variable} ${albert_sans.variable}`;