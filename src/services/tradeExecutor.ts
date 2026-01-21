import { ClobClient } from '@polymarket/clob-client';
import { UserActivityInterface, UserPositionInterface } from '../interfaces/User';
import { ENV } from '../config/env';
import { getUserActivityModel } from '../models/userHistory';
import fetchData from '../utils/fetchData';
import getMyBalance from '../utils/getMyBalance';
import postOrder from '../utils/postOrder';
import Logger from '../utils/logger';
import { calculateOrderSize } from '../config/copyStrategy';

const USER_ADDRESSES = ENV.USER_ADDRESSES;
const RETRY_LIMIT = ENV.RETRY_LIMIT;
const PROXY_WALLET = ENV.PROXY_WALLET;
const TRADE_AGGREGATION_ENABLED = ENV.TRADE_AGGREGATION_ENABLED;
const COPY_STRATEGY_CONFIG = ENV.COPY_STRATEGY_CONFIG;

// Threshold-based aggregation: execute when either threshold is met
// This eliminates skips - trades aggregate until they can be executed
const AGGREGATION_TAKER_MIN_TOKENS = 5;   // Taker orders need >= 5 tokens
const AGGREGATION_MAKER_MIN_USD = 1.00;   // Maker orders need >= $1 USD

// Market filter: only process trades from these markets (empty array = all markets)
// Matches against trade.slug or trade.eventSlug
const ALLOWED_MARKETS: string[] = [
    'btc-updown-15m',
    'eth-updown-15m',
    // Add more market slugs here as needed
];

/**
 * Check if a trade is from an allowed market
 */
const isAllowedMarket = (trade: TradeWithUser): boolean => {
    // If no filter configured, allow all markets
    if (ALLOWED_MARKETS.length === 0) return true;

    const tradeSlug = trade.slug?.toLowerCase() || '';
    const tradeEventSlug = trade.eventSlug?.toLowerCase() || '';

    return ALLOWED_MARKETS.some(market => {
        const marketLower = market.toLowerCase();
        return tradeSlug.includes(marketLower) || tradeEventSlug.includes(marketLower);
    });
};

// Create activity models for each user
const userActivityModels = USER_ADDRESSES.map((address) => ({
    address,
    model: getUserActivityModel(address),
}));

interface TradeWithUser extends UserActivityInterface {
    userAddress: string;
    scaledUsdcSize?: number; // Scaled size based on copy strategy
}

interface AggregatedTrade {
    userAddress: string;
    conditionId: string;
    asset: string;
    side: string;
    slug?: string;
    eventSlug?: string;
    trades: TradeWithUser[];
    totalRawUsdcSize: number; // Raw trader size (for logging)
    totalScaledUsdcSize: number; // Scaled size based on copy strategy (for threshold checks)
    averagePrice: number;
    firstTradeTime: number;
    lastTradeTime: number;
}

// Buffer for aggregating trades
const tradeAggregationBuffer: Map<string, AggregatedTrade> = new Map();

const readTempTrades = async (): Promise<TradeWithUser[]> => {
    const allTrades: TradeWithUser[] = [];

    for (const { address, model } of userActivityModels) {
        // Only get trades that haven't been processed yet (bot: false AND botExcutedTime: 0)
        // This prevents processing the same trade multiple times
        const trades = await model
            .find({
                $and: [{ type: 'TRADE' }, { bot: false }, { botExcutedTime: 0 }],
            })
            .exec();

        const tradesWithUser = trades.map((trade) => ({
            ...(trade.toObject() as UserActivityInterface),
            userAddress: address,
        }));

        allTrades.push(...tradesWithUser);
    }

    return allTrades;
};

/**
 * Generate a unique key for trade aggregation based on user, market, side
 */
const getAggregationKey = (trade: TradeWithUser): string => {
    return `${trade.userAddress}:${trade.conditionId}:${trade.asset}:${trade.side}`;
};

/**
 * Add trade to aggregation buffer or update existing aggregation
 * Uses scaledUsdcSize (must be pre-calculated) for threshold tracking
 */
const addToAggregationBuffer = (trade: TradeWithUser): void => {
    const key = getAggregationKey(trade);
    const existing = tradeAggregationBuffer.get(key);
    const now = Date.now();
    const scaledSize = trade.scaledUsdcSize ?? trade.usdcSize;

    if (existing) {
        // Update existing aggregation
        existing.trades.push(trade);
        existing.totalRawUsdcSize += trade.usdcSize;
        existing.totalScaledUsdcSize += scaledSize;
        // Recalculate weighted average price (using raw sizes for price averaging)
        const totalValue = existing.trades.reduce((sum, t) => sum + t.usdcSize * t.price, 0);
        existing.averagePrice = totalValue / existing.totalRawUsdcSize;
        existing.lastTradeTime = now;
    } else {
        // Create new aggregation
        tradeAggregationBuffer.set(key, {
            userAddress: trade.userAddress,
            conditionId: trade.conditionId,
            asset: trade.asset,
            side: trade.side || 'BUY',
            slug: trade.slug,
            eventSlug: trade.eventSlug,
            trades: [trade],
            totalRawUsdcSize: trade.usdcSize,
            totalScaledUsdcSize: scaledSize,
            averagePrice: trade.price,
            firstTradeTime: now,
            lastTradeTime: now,
        });
    }
};

/**
 * Check buffer and return ready aggregated trades
 * Threshold-based triggering: ready when token threshold is met:
 * - Total scaled tokens >= 5 (required for GTC limit orders)
 *
 * Trades keep aggregating until 5 token minimum is reached
 */
const getReadyAggregatedTrades = (): AggregatedTrade[] => {
    const ready: AggregatedTrade[] = [];

    for (const [key, agg] of tradeAggregationBuffer.entries()) {
        // Calculate total scaled tokens from USD and average price
        const totalScaledTokens = agg.averagePrice > 0
            ? agg.totalScaledUsdcSize / agg.averagePrice
            : 0;

        // Only trigger when token threshold is met (5 token minimum for orders)
        if (totalScaledTokens >= AGGREGATION_TAKER_MIN_TOKENS) {
            Logger.info(
                `✅ Aggregation ready: ${agg.trades.length} trades on ${agg.slug || agg.asset} | ${totalScaledTokens.toFixed(2)} tokens >= ${AGGREGATION_TAKER_MIN_TOKENS}`
            );

            ready.push(agg);
            tradeAggregationBuffer.delete(key);
        }
        // If threshold not met, keep in buffer until more trades arrive
    }

    return ready;
};

const doTrading = async (clobClient: ClobClient, trades: TradeWithUser[]) => {
    for (const trade of trades) {
        try {
            // Mark trade as being processed immediately to prevent duplicate processing
            const UserActivity = getUserActivityModel(trade.userAddress);
            await UserActivity.updateOne({ _id: trade._id }, { $set: { botExcutedTime: 1 } });

            Logger.trade(trade.userAddress, trade.side || 'UNKNOWN', {
                asset: trade.asset,
                side: trade.side,
                amount: trade.usdcSize,
                price: trade.price,
                slug: trade.slug,
                eventSlug: trade.eventSlug,
                transactionHash: trade.transactionHash,
            });

            const my_positions: UserPositionInterface[] = await fetchData(
                `https://data-api.polymarket.com/positions?user=${PROXY_WALLET}`
            );
            const user_positions: UserPositionInterface[] = await fetchData(
                `https://data-api.polymarket.com/positions?user=${trade.userAddress}`
            );
            const my_position = my_positions.find(
                (position: UserPositionInterface) => position.conditionId === trade.conditionId
            );
            const user_position = user_positions.find(
                (position: UserPositionInterface) => position.conditionId === trade.conditionId
            );

            // Get USDC balance
            const my_balance = await getMyBalance(PROXY_WALLET);

            // Calculate trader's total portfolio value from positions
            const user_balance = user_positions.reduce((total, pos) => {
                return total + (pos.currentValue || 0);
            }, 0);

            Logger.balance(my_balance, user_balance, trade.userAddress);

            // Execute the trade
            await postOrder(
                clobClient,
                trade.side === 'BUY' ? 'buy' : 'sell',
                my_position,
                user_position,
                trade,
                my_balance,
                user_balance,
                trade.userAddress
            );

            Logger.separator();
        } catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            Logger.warning(`⚠️ Error executing trade: ${errMsg}`);
            // Mark trade as processed to avoid infinite retry
            const UserActivity = getUserActivityModel(trade.userAddress);
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3s before next
        }
    }
};

/**
 * Execute aggregated trades
 */
const doAggregatedTrading = async (clobClient: ClobClient, aggregatedTrades: AggregatedTrade[]) => {
    for (const agg of aggregatedTrades) {
        try {
            Logger.header(`📊 AGGREGATED TRADE (${agg.trades.length} trades combined)`);
            Logger.info(`Market: ${agg.slug || agg.asset}`);
            Logger.info(`Side: ${agg.side}`);
            Logger.info(`Total raw volume: $${agg.totalRawUsdcSize.toFixed(2)} → Scaled: $${agg.totalScaledUsdcSize.toFixed(2)}`);
            Logger.info(`Average price: $${agg.averagePrice.toFixed(4)}`);

            // Mark all individual trades as being processed
            for (const trade of agg.trades) {
                const UserActivity = getUserActivityModel(trade.userAddress);
                await UserActivity.updateOne({ _id: trade._id }, { $set: { botExcutedTime: 1 } });
            }

            const my_positions: UserPositionInterface[] = await fetchData(
                `https://data-api.polymarket.com/positions?user=${PROXY_WALLET}`
            );
            const user_positions: UserPositionInterface[] = await fetchData(
                `https://data-api.polymarket.com/positions?user=${agg.userAddress}`
            );
            const my_position = my_positions.find(
                (position: UserPositionInterface) => position.conditionId === agg.conditionId
            );
            const user_position = user_positions.find(
                (position: UserPositionInterface) => position.conditionId === agg.conditionId
            );

            // Get USDC balance
            const my_balance = await getMyBalance(PROXY_WALLET);

            // Calculate trader's total portfolio value from positions
            const user_balance = user_positions.reduce((total, pos) => {
                return total + (pos.currentValue || 0);
            }, 0);

            Logger.balance(my_balance, user_balance, agg.userAddress);

            // Create a synthetic trade object for postOrder
            const syntheticTrade: UserActivityInterface = {
                ...agg.trades[0], // Use first trade as template
                usdcSize: agg.totalRawUsdcSize, // Raw for logging
                price: agg.averagePrice,
                side: agg.side as 'BUY' | 'SELL',
            };

            // Execute the aggregated trade with pre-scaled amount (skip double-scaling)
            await postOrder(
                clobClient,
                agg.side === 'BUY' ? 'buy' : 'sell',
                my_position,
                user_position,
                syntheticTrade,
                my_balance,
                user_balance,
                agg.userAddress,
                agg.totalScaledUsdcSize // Pass pre-scaled amount to skip calculateOrderSize
            );

            Logger.separator();
        } catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            Logger.warning(`⚠️ Error executing aggregated trade: ${errMsg}`);
            // Mark trades as processed anyway to avoid infinite retry
            for (const trade of agg.trades) {
                const UserActivity = getUserActivityModel(trade.userAddress);
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            }
            await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3s before next
        }
    }
};

// Track if executor should continue running
let isRunning = true;

/**
 * Stop the trade executor gracefully
 */
export const stopTradeExecutor = () => {
    isRunning = false;
    Logger.info('Trade executor shutdown requested...');
};

const tradeExecutor = async (clobClient: ClobClient) => {
    Logger.success(`Trade executor ready for ${USER_ADDRESSES.length} trader(s)`);
    if (ALLOWED_MARKETS.length > 0) {
        Logger.info(`🎯 Market filter active: ${ALLOWED_MARKETS.join(', ')}`);
    }
    if (TRADE_AGGREGATION_ENABLED) {
        Logger.info(
            `📊 Threshold-based aggregation: execute when >= ${AGGREGATION_TAKER_MIN_TOKENS} tokens reached`
        );
        Logger.info(`   → Trades accumulate until 5 token minimum met`);
    }

    let lastCheck = Date.now();
    while (isRunning) {
        const allTrades = await readTempTrades();

        // Filter trades by allowed markets
        const trades: TradeWithUser[] = [];
        const filteredOutTrades: TradeWithUser[] = [];

        for (const trade of allTrades) {
            if (isAllowedMarket(trade)) {
                trades.push(trade);
            } else {
                filteredOutTrades.push(trade);
            }
        }

        // Mark filtered-out trades as processed so they don't reappear
        if (filteredOutTrades.length > 0) {
            for (const trade of filteredOutTrades) {
                const UserActivity = getUserActivityModel(trade.userAddress);
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            }
            Logger.info(`🚫 Filtered out ${filteredOutTrades.length} trade(s) from non-allowed markets`);
        }

        if (TRADE_AGGREGATION_ENABLED) {
            try {
                // Process with aggregation logic
                if (trades.length > 0) {
                    Logger.clearLine();
                    Logger.info(
                        `📥 ${trades.length} new trade${trades.length > 1 ? 's' : ''} detected from allowed markets`
                    );

                    // Fetch balances once for scaling calculations
                    const my_balance = await getMyBalance(PROXY_WALLET);
                    const my_positions: UserPositionInterface[] = await fetchData(
                        `https://data-api.polymarket.com/positions?user=${PROXY_WALLET}`
                    );

                    // Add ALL BUY trades to aggregation buffer (threshold-based execution)
                    for (const trade of trades) {
                        // Calculate scaled size for this trade
                        const my_position = my_positions.find(
                            (pos) => pos.conditionId === trade.conditionId
                        );
                        const currentPositionValue = my_position ? my_position.size * my_position.avgPrice : 0;

                        const orderCalc = calculateOrderSize(
                            COPY_STRATEGY_CONFIG,
                            trade.usdcSize,
                            my_balance,
                            currentPositionValue
                        );

                        // Store scaled size on trade for aggregation buffer
                        trade.scaledUsdcSize = orderCalc.finalAmount;

                        if (trade.side === 'BUY') {
                            // All BUY trades go to aggregation buffer
                            const scaledTokens = orderCalc.finalAmount / trade.price;
                            Logger.info(
                                `📥 Buffering: $${trade.usdcSize.toFixed(2)} raw → $${orderCalc.finalAmount.toFixed(2)} / ${scaledTokens.toFixed(2)} tokens for ${trade.slug || trade.asset}`
                            );
                            // Mark trade as being processed to prevent re-reading on next poll
                            const UserActivity = getUserActivityModel(trade.userAddress);
                            await UserActivity.updateOne({ _id: trade._id }, { $set: { botExcutedTime: 1 } });
                            addToAggregationBuffer(trade);
                        } else {
                            // SELL trades execute immediately (don't aggregate sells)
                            Logger.clearLine();
                            Logger.header(`⚡ SELL TRADE`);
                            await doTrading(clobClient, [trade]);
                        }
                    }
                    lastCheck = Date.now();
                }

                // Check for ready aggregated trades
                const readyAggregations = getReadyAggregatedTrades();
                if (readyAggregations.length > 0) {
                    Logger.clearLine();
                    Logger.header(
                        `⚡ ${readyAggregations.length} AGGREGATED TRADE${readyAggregations.length > 1 ? 'S' : ''} READY`
                    );
                    await doAggregatedTrading(clobClient, readyAggregations);
                    lastCheck = Date.now();
                }
            } catch (err) {
                // Handle network/timeout errors gracefully - don't crash the loop
                const errMsg = err instanceof Error ? err.message : String(err);
                if (errMsg.includes('timeout') || errMsg.includes('ETIMEDOUT') || errMsg.includes('network')) {
                    Logger.warning(`⚠️ Network timeout - will retry on next cycle`);
                    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5s before continuing
                } else {
                    Logger.warning(`⚠️ Error processing trades: ${errMsg}`);
                    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2s
                }
            }

            // Update waiting message with buffer status
            if (trades.length === 0) {
                if (Date.now() - lastCheck > 300) {
                    const bufferedCount = tradeAggregationBuffer.size;
                    if (bufferedCount > 0) {
                        // Calculate total buffered amounts across all groups
                        let totalBufferedUsd = 0;
                        let totalBufferedTokens = 0;
                        for (const agg of tradeAggregationBuffer.values()) {
                            totalBufferedUsd += agg.totalScaledUsdcSize;
                            if (agg.averagePrice > 0) {
                                totalBufferedTokens += agg.totalScaledUsdcSize / agg.averagePrice;
                            }
                        }
                        Logger.waiting(
                            USER_ADDRESSES.length,
                            `${bufferedCount} group(s) buffered: $${totalBufferedUsd.toFixed(2)} / ${totalBufferedTokens.toFixed(1)} tokens`
                        );
                    } else {
                        Logger.waiting(USER_ADDRESSES.length);
                    }
                    lastCheck = Date.now();
                }
            }
        } else {
            // Original non-aggregation logic
            if (trades.length > 0) {
                Logger.clearLine();
                Logger.header(
                    `⚡ ${trades.length} NEW TRADE${trades.length > 1 ? 'S' : ''} TO COPY`
                );
                await doTrading(clobClient, trades);
                lastCheck = Date.now();
            } else {
                // Update waiting message every 300ms for smooth animation
                if (Date.now() - lastCheck > 300) {
                    Logger.waiting(USER_ADDRESSES.length);
                    lastCheck = Date.now();
                }
            }
        }

        if (!isRunning) break;
        await new Promise((resolve) => setTimeout(resolve, 300));
    }

    Logger.info('Trade executor stopped');
};

export default tradeExecutor;