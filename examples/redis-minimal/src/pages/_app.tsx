import type { AppProps } from "next/app";
import { Geist, Geist_Mono } from "next/font/google";
import "../app/globals.css";
import { NavigationPages } from "@/components/NavigationPages";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export default function App({ Component, pageProps }: AppProps) {
  return (
    <div className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
      <div className="flex">
        <NavigationPages />
        <div className="flex-1 lg:pl-64">
          <Component {...pageProps} />
        </div>
      </div>
    </div>
  );
}

