"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.logger = exports.config = void 0;
const pino_1 = __importDefault(require("pino"));
exports.config = {
    postgres: {
        host: process.env.PG_HOST || 'localhost',
        port: parseInt(process.env.PG_PORT || '5432'),
        database: process.env.PG_DATABASE || 'tradfi_web3',
        user: process.env.PG_USER || 'app_writer',
        password: process.env.PG_PASSWORD || 'password',
        max: parseInt(process.env.PG_POOL_MAX || '20'),
    },
    redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT || '6379'),
        password: process.env.REDIS_PASSWORD || undefined,
    },
    kafka: {
        brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
        clientId: process.env.KAFKA_CLIENT_ID || 'tradfi-web3',
    },
    ethereum: {
        rpcUrl: process.env.ETH_RPC_URL || 'http://localhost:8545',
        chainId: parseInt(process.env.ETH_CHAIN_ID || '1'),
        confirmations: parseInt(process.env.ETH_CONFIRMATIONS || '12'),
    },
    server: {
        port: parseInt(process.env.PORT || '3000'),
    },
};
exports.logger = (0, pino_1.default)({ level: process.env.LOG_LEVEL || 'info' });
//# sourceMappingURL=config.js.map