export declare const config: {
    postgres: {
        host: string;
        port: number;
        database: string;
        user: string;
        password: string;
        max: number;
    };
    redis: {
        host: string;
        port: number;
        password: string | undefined;
    };
    kafka: {
        brokers: string[];
        clientId: string;
    };
    ethereum: {
        rpcUrl: string;
        chainId: number;
        confirmations: number;
    };
    server: {
        port: number;
    };
};
export declare const logger: import("pino").Logger<never, boolean>;
//# sourceMappingURL=config.d.ts.map