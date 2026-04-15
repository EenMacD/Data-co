import { ReactElement } from "react";

export default interface PinnedFilter {
    id: string;
    label: string;
    text: string;
    template: ReactElement;
}
