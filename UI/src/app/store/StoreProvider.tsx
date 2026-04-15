// src/app/StoreProvider.tsx
"use client";

import { useState } from "react";
import { Provider } from "react-redux";
import { makeStore, type AppStore } from "@/app/store/store";

export default function StoreProvider({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    const [store] = useState<AppStore>(() => makeStore());

    return <Provider store={store}>{children}</Provider>;
}
