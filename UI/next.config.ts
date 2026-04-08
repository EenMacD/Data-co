import type { NextConfig } from "next";

const nextConfig: NextConfig = {
    turbopack: {
        rules: {
            "*.svg": {
                loaders: ["@svgr/webpack"],
                as: "*.js",
            },
        },
    },
    webpack(
        config: Parameters<NonNullable<NextConfig["webpack"]>>[0],
    ): Parameters<NonNullable<NextConfig["webpack"]>>[0] {
        config.module.rules.push({
            test: /\.svg$/,
            use: ["@svgr/webpack"],
        });

        return config;
    },
};

export default nextConfig;
