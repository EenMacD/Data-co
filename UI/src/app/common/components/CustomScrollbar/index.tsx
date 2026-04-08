"use client";

import type { MouseEvent as ReactMouseEvent, ReactElement, RefObject } from "react";
import { useEffect, useRef, useState, useCallback } from "react";
import styles from "./styles.module.css";

interface CustomScrollbarProps {
    scrollContainerRef: RefObject<HTMLDivElement | null>;
}

export default function CustomScrollbar({
    scrollContainerRef,
}: CustomScrollbarProps): ReactElement {
    const trackRef: RefObject<HTMLDivElement | null> =
        useRef<HTMLDivElement>(null);
    const thumbRef: RefObject<HTMLDivElement | null> =
        useRef<HTMLDivElement>(null);
    const [isDragging, setIsDragging] = useState(false);
    const [startX, setStartX] = useState(0);
    const [scrollLeftStart, setScrollLeftStart] = useState(0);

    const updateThumb: () => void = useCallback((): void => {
        if (
            !scrollContainerRef.current ||
            !trackRef.current ||
            !thumbRef.current
        )
            return;

        const { scrollLeft, scrollWidth, clientWidth } =
            scrollContainerRef.current;
        const trackWidth: number = trackRef.current.clientWidth;

        // Calculate thumb width based on visible ratio
        const visibleRatio: number = clientWidth / scrollWidth;
        const thumbWidth: number = Math.max(visibleRatio * trackWidth, 20); // Min width 20px

        // Calculate thumb position
        const maxScrollLeft: number = scrollWidth - clientWidth;
        const scrollRatio: number = scrollLeft / maxScrollLeft;
        const maxThumbLeft: number = trackWidth - thumbWidth;
        const thumbLeft: number = scrollRatio * maxThumbLeft;

        thumbRef.current.style.width = `${thumbWidth}px`;
        thumbRef.current.style.transform = `translateX(${thumbLeft}px)`;

        // Hide scrollbar if content fits
        if (visibleRatio >= 1) {
            trackRef.current.style.opacity = "0";
            trackRef.current.style.pointerEvents = "none";
        } else {
            trackRef.current.style.opacity = "1";
            trackRef.current.style.pointerEvents = "auto";
        }
    }, [scrollContainerRef]);

    // Update on scroll
    useEffect(() => {
        const container: HTMLDivElement | null = scrollContainerRef.current;
        if (!container) return;

        const handleScroll: () => void = (): void => {
            if (!isDragging) {
                requestAnimationFrame(updateThumb);
            }
        };

        // Initial update
        updateThumb();

        // Also update on resize of container AND content
        const resizeObserver: ResizeObserver = new ResizeObserver((): void => {
            updateThumb();
        });

        resizeObserver.observe(container);
        if (container.firstElementChild) {
            resizeObserver.observe(container.firstElementChild);
        }

        container.addEventListener("scroll", handleScroll);
        window.addEventListener("resize", updateThumb);

        return (): void => {
            container.removeEventListener("scroll", handleScroll);
            window.removeEventListener("resize", updateThumb);
            resizeObserver.disconnect();
        };
    }, [scrollContainerRef, isDragging, updateThumb]);

    // Handle drag logic
    const handleMouseDown: (e: ReactMouseEvent) => void = (
        e: ReactMouseEvent,
    ): void => {
        if (!scrollContainerRef.current || !thumbRef.current) return;
        e.preventDefault();
        setIsDragging(true);
        setStartX(e.clientX);
        // Store the initial thumb position's corresponding scrollLeft isn't quite right,
        // better to store the initial scrollLeft
        setScrollLeftStart(scrollContainerRef.current.scrollLeft);

        document.body.style.userSelect = "none";
    };

    useEffect(() => {
        if (!isDragging) return;

        const handleMouseMove: (e: MouseEvent) => void = (
            e: MouseEvent,
        ): void => {
            if (
                !scrollContainerRef.current ||
                !trackRef.current ||
                !thumbRef.current
            )
                return;

            const deltaX: number = e.clientX - startX;
            const trackWidth: number = trackRef.current.clientWidth;
            const { scrollWidth, clientWidth } = scrollContainerRef.current;

            const visibleRatio: number = clientWidth / scrollWidth;
            const thumbWidth: number = Math.max(visibleRatio * trackWidth, 20);
            const maxThumbLeft: number = trackWidth - thumbWidth;
            const maxScrollLeft: number = scrollWidth - clientWidth;

            // Calculate new scroll position based on delta
            // thumbMove / maxThumbMove = scrollMove / maxScrollMove
            // scrollMove = (thumbMove * maxScrollMove) / maxThumbMove

            // However, dragging by pixels on track corresponds to pixels on scroll content
            const scrollAmount: number = (deltaX / maxThumbLeft) * maxScrollLeft;

            scrollContainerRef.current.scrollLeft =
                scrollLeftStart + scrollAmount;

            // Manually update thumb position for smoothness during modify
            // But since source of truth is scrollLeft, we can rely on the scroll event listener?
            // Actually, for smoother 60fps, we might want to update here too.
            // But let's let the scroll listener handle it to keep single source of truth (scrollLeft).
            // Wait, scroll listener is disabled during dragging to avoid loops?
            // - No, "if (!isDragging)" check in handleScroll prevents fighting.
            // So we MUST update thumb here.

            updateThumb();
        };

        const handleMouseUp: () => void = (): void => {
            setIsDragging(false);
            document.body.style.userSelect = "";
        };

        document.addEventListener("mousemove", handleMouseMove);
        document.addEventListener("mouseup", handleMouseUp);

        return (): void => {
            document.removeEventListener("mousemove", handleMouseMove);
            document.removeEventListener("mouseup", handleMouseUp);
        };
    }, [isDragging, startX, scrollLeftStart, scrollContainerRef, updateThumb]);

    // Handle click on track to jump
    const handleTrackClick: (e: ReactMouseEvent) => void = (
        e: ReactMouseEvent,
    ): void => {
        if (e.target === thumbRef.current) return; // Ignore clicks on the thumb itself
        if (
            !scrollContainerRef.current ||
            !trackRef.current ||
            !thumbRef.current
        )
            return;

        const trackRect: DOMRect = trackRef.current.getBoundingClientRect();
        const clickX: number = e.clientX - trackRect.left;

        const trackWidth: number = trackRef.current.clientWidth;
        const { scrollWidth, clientWidth } = scrollContainerRef.current;
        const visibleRatio: number = clientWidth / scrollWidth;
        const thumbWidth: number = Math.max(visibleRatio * trackWidth, 20);

        // Center the thumb on the click position
        // improved logic: calculate the desired center of thumb
        const desiredThumbCenter: number = clickX;
        const desiredThumbLeft: number = desiredThumbCenter - thumbWidth / 2;

        const maxThumbLeft: number = trackWidth - thumbWidth;
        // Clamp
        const clampedThumbLeft: number = Math.max(
            0,
            Math.min(desiredThumbLeft, maxThumbLeft),
        );

        const maxScrollLeft: number = scrollWidth - clientWidth;
        const scrollRatio: number = clampedThumbLeft / maxThumbLeft;

        scrollContainerRef.current.scrollTo({
            left: scrollRatio * maxScrollLeft,
            behavior: "smooth",
        });
    };

    return (
        <div className={styles.container}>
            <div
                className={styles.scrollbarTrack}
                ref={trackRef}
                onClick={handleTrackClick}
            >
                <div
                    className={styles.scrollbarThumb}
                    ref={thumbRef}
                    onMouseDown={handleMouseDown}
                />
            </div>
        </div>
    );
}
