import { configureStore } from "@reduxjs/toolkit";
import companiesReducer from "../companies/store/features/searchCompaniesSlice";

export const makeStore = () =>
    configureStore({
        reducer: {
            companiesSearch: companiesReducer,
        },
    });

export type AppStore = ReturnType<typeof makeStore>;
export type RootState = ReturnType<AppStore["getState"]>;
export type AppDispatch = AppStore["dispatch"];
