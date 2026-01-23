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
const PROXY_WALLET = ENV.PROXY_WALLET;
const TRADE_AGGREGATION_ENABLED = ENV.TRADE_AGGREGATION_ENABLED;
const COPY_STRATEGY_CONFIG = ENV.COPY_STRATEGY_CONFIG;

// Minimum tokens for GTC orders
const MIN_TOKENS_FOR_ORDER = 5;

// Arb thresholds - checked at EXECUTION time, not aggregation time
const ARB_MAX_COST_BASIS = 0.95;    // Combined YES+NO avg price must be < 95¢
const ARB_MAX_IMBALANCE = 0.25;     // Max 25% qty imbalance between sides

// Market filter: only process trades from these markets
// Uses flexible matching - just needs to contain these keywords
const ALLOWED_MARKET_KEYWORDS: string[][] = [
    ['btc', '15m'],   // Must contain both 'btc' AND '15m'
    ['eth', '15m'],   // Must contain both 'eth' AND '15m'
];

// Create activity models for each user
const userActivityModels = USER_ADDRESSES.map((address) => ({
    address,
    model: getUserActivityModel(address),
}));

interface TradeWithUser extends UserActivityInterface {
    userAddress: string;
    scaledUsdcSize?: number;
}

interface AggregatedTrade {
    userAddress: string;
    conditionId: string;
    asset: string;
    side: string;
    slug?: string;
    eventSlug?: string;
    trades: TradeWithUser[];
    totalRawUsdcSize: number;
    totalScaledUsdcSize: number;
    averagePrice: number;
    firstTradeTime: number;
    lastTradeTime: number;
}

// ============================================================================
// SIMPLE AGGREGATION BUFFER - groups trades by conditionId + asset
// ============================================================================
const tradeAggregationBuffer: Map<string, AggregatedTrade> = new Map();

// ============================================================================
// MARKET TRACKER - tracks BOTH sides for each conditionId (discovers pairs)
// Each conditionId can have 2 assets (YES and NO) - we discover them as trades come in
// ============================================================================
interface AssetExecution {
    asset: string;
    quantity: number;
    totalCost: number;
    avgPrice: number;
}

interface MarketTracker {
    conditionId: string;
    slug?: string;
    // We track up to 2 assets per market (YES and NO)
    // Key is asset ID, value is execution data
    assets: Map<string, AssetExecution>;
}

// Track by conditionId - this naturally separates 15-min windows
const marketTrackers: Map<string, MarketTracker> = new Map();

/**
 * Get or create market tracker for a conditionId
 */
const getMarketTracker = (conditionId: string, slug?: string): MarketTracker => {
    let tracker = marketTrackers.get(conditionId);
    if (!tracker) {
        tracker = { conditionId, slug, assets: new Map() };
        marketTrackers.set(conditionId, tracker);
    }
    if (slug && !tracker.slug) {
        tracker.slug = slug;
    }
    return tracker;
};

/**
 * Record an executed trade and return the current market state
 */
const recordExecution = (conditionId: string, asset: string, quantity: number, cost: number, slug?: string): void => {
    Logger.info(`📝 RECORDING EXECUTION:`);
    Logger.info(`   conditionId: ${conditionId}`);
    Logger.info(`   asset: ${asset}`);
    Logger.info(`   quantity: ${quantity.toFixed(2)}, cost: $${cost.toFixed(2)}`);

    const tracker = getMarketTracker(conditionId, slug);
    const existing = tracker.assets.get(asset);

    if (existing) {
        existing.quantity += quantity;
        existing.totalCost += cost;
        existing.avgPrice = existing.totalCost / existing.quantity;
        Logger.info(`   Updated existing side`);
    } else {
        tracker.assets.set(asset, {
            asset,
            quantity,
            totalCost: cost,
            avgPrice: cost / quantity,
        });
        Logger.info(`   Created NEW side (${tracker.assets.size} sides now in this market)`);
    }

    // Log the FULL market state after recording
    Logger.info(`📊 MARKET TRACKER STATE for ${tracker.slug || conditionId}:`);
    Logger.info(`   conditionId: ${conditionId}`);
    Logger.info(`   Total sides tracked: ${tracker.assets.size}`);
    const assetList = Array.from(tracker.assets.entries());
    for (let i = 0; i < assetList.length; i++) {
        const [assetId, data] = assetList[i];
        Logger.info(`   Side ${i + 1}: asset=${assetId.slice(0, 16)}... | ${data.quantity.toFixed(2)} tokens @ $${data.avgPrice.toFixed(4)}`);
    }
    if (assetList.length === 2) {
        const costBasis = assetList[0][1].avgPrice + assetList[1][1].avgPrice;
        const imbalance = Math.abs(assetList[0][1].quantity - assetList[1][1].quantity) / (assetList[0][1].quantity + assetList[1][1].quantity);
        Logger.info(`   ✅ PAIRED! Cost basis: $${costBasis.toFixed(4)} | Imbalance: ${(imbalance * 100).toFixed(1)}%`);
    } else {
        Logger.info(`   ⏳ Waiting for opposite side...`);
    }

    // Log total trackers
    Logger.info(`📊 TOTAL MARKET TRACKERS: ${marketTrackers.size}`);
};

/**
 * Check arb constraints before execution
 * Only reject if trade makes things WORSE:
 * - Reject if it pushes cost basis ABOVE 95¢ (when it was below)
 * - Reject if it makes imbalance WORSE (not if it helps balance)
 */
const checkArbBeforeExecution = (
    conditionId: string,
    thisAsset: string,
    newQuantity: number,
    newAvgPrice: number,
    slug?: string
): { ok: boolean; reason: string; costBasis: number; imbalance: number } => {
    Logger.info(`🔍 ARB CHECK:`);
    Logger.info(`   conditionId: ${conditionId}`);
    Logger.info(`   thisAsset: ${thisAsset}`);
    Logger.info(`   newQty: ${newQuantity.toFixed(2)} @ $${newAvgPrice.toFixed(4)}`);

    const tracker = getMarketTracker(conditionId, slug);
    Logger.info(`   Tracker has ${tracker.assets.size} existing side(s)`);

    // Get what we've already executed for THIS asset
    const thisExisting = tracker.assets.get(thisAsset);

    // Current state (before this trade)
    const currentThisQty = thisExisting?.quantity || 0;
    const currentThisCost = thisExisting?.totalCost || 0;

    // New state (after this trade)
    const newThisQty = currentThisQty + newQuantity;
    const newThisCost = currentThisCost + (newQuantity * newAvgPrice);
    const newThisAvgPrice = newThisCost / newThisQty;

    // Find the OTHER asset (if any)
    let otherAsset: AssetExecution | undefined;
    for (const [asset, data] of tracker.assets.entries()) {
        if (asset !== thisAsset) {
            otherAsset = data;
            break;
        }
    }

    // If we don't have another side yet, this is first side - always allow
    if (!otherAsset || otherAsset.quantity === 0) {
        Logger.info(`   First side for this market - allowing`);
        return { ok: true, reason: 'First side - no opposite yet', costBasis: 0, imbalance: 0 };
    }

    const otherQty = otherAsset.quantity;
    const otherAvgPrice = otherAsset.avgPrice;

    // Calculate CURRENT state (before trade)
    const currentThisAvgPrice = currentThisQty > 0 ? currentThisCost / currentThisQty : 0;
    const currentCostBasis = currentThisAvgPrice + otherAvgPrice;
    const currentTotalQty = currentThisQty + otherQty;
    const currentImbalance = currentTotalQty > 0 ? Math.abs(currentThisQty - otherQty) / currentTotalQty : 0;

    // Calculate NEW state (after trade)
    const newCostBasis = newThisAvgPrice + otherAvgPrice;
    const newTotalQty = newThisQty + otherQty;
    const newImbalance = Math.abs(newThisQty - otherQty) / newTotalQty;

    Logger.info(`   Current: this=${currentThisQty.toFixed(2)} other=${otherQty.toFixed(2)} | cost=$${currentCostBasis.toFixed(4)} | imbal=${(currentImbalance * 100).toFixed(1)}%`);
    Logger.info(`   After:   this=${newThisQty.toFixed(2)} other=${otherQty.toFixed(2)} | cost=$${newCostBasis.toFixed(4)} | imbal=${(newImbalance * 100).toFixed(1)}%`);

    // ONLY reject if trade makes cost basis go ABOVE threshold (when it was below)
    if (newCostBasis >= ARB_MAX_COST_BASIS && currentCostBasis < ARB_MAX_COST_BASIS) {
        return {
            ok: false,
            reason: `Would push cost basis above ${ARB_MAX_COST_BASIS}: $${currentCostBasis.toFixed(4)} → $${newCostBasis.toFixed(4)}`,
            costBasis: newCostBasis,
            imbalance: newImbalance,
        };
    }

    // ONLY reject if trade makes imbalance WORSE
    if (newImbalance > currentImbalance && newImbalance > ARB_MAX_IMBALANCE) {
        return {
            ok: false,
            reason: `Would worsen imbalance: ${(currentImbalance * 100).toFixed(1)}% → ${(newImbalance * 100).toFixed(1)}%`,
            costBasis: newCostBasis,
            imbalance: newImbalance,
        };
    }

    // Trade is OK - either improves things or keeps them acceptable
    let reason = 'OK';
    if (newImbalance < currentImbalance) {
        reason = `Improves balance: ${(currentImbalance * 100).toFixed(1)}% → ${(newImbalance * 100).toFixed(1)}%`;
    } else if (newCostBasis < currentCostBasis) {
        reason = `Improves cost basis: $${currentCostBasis.toFixed(4)} → $${newCostBasis.toFixed(4)}`;
    }

    return { ok: true, reason, costBasis: newCostBasis, imbalance: newImbalance };
};

/**
 * Check if a trade is from an allowed market
 * Uses flexible keyword matching - trade must contain ALL keywords in at least one group
 */
const isAllowedMarket = (trade: TradeWithUser): boolean => {
    if (ALLOWED_MARKET_KEYWORDS.length === 0) return true;

    const tradeSlug = trade.slug?.toLowerCase() || '';
    const tradeEventSlug = trade.eventSlug?.toLowerCase() || '';
    const combined = tradeSlug + ' ' + tradeEventSlug;

    // Check if ANY keyword group matches (all keywords in the group must be present)
    return ALLOWED_MARKET_KEYWORDS.some(keywords => {
        return keywords.every(keyword => combined.includes(keyword.toLowerCase()));
    });
};

const readTempTrades = async (): Promise<TradeWithUser[]> => {
    const allTrades: TradeWithUser[] = [];
    for (const { address, model } of userActivityModels) {
        const trades = await model
            .find({ $and: [{ type: 'TRADE' }, { bot: false }, { botExcutedTime: 0 }] })
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
 * Generate aggregation key - by conditionId + asset (side)
 */
const getAggregationKey = (trade: TradeWithUser): string => {
    return `${trade.userAddress}:${trade.conditionId}:${trade.asset}`;
};

/**
 * Add trade to aggregation buffer - NO REJECTION, just buffer
 */
const addToAggregationBuffer = (trade: TradeWithUser): void => {
    const key = getAggregationKey(trade);
    const existing = tradeAggregationBuffer.get(key);
    const now = Date.now();
    const scaledSize = trade.scaledUsdcSize ?? trade.usdcSize;

    if (existing) {
        existing.trades.push(trade);
        existing.totalRawUsdcSize += trade.usdcSize;
        existing.totalScaledUsdcSize += scaledSize;
        const totalValue = existing.trades.reduce((sum, t) => sum + t.usdcSize * t.price, 0);
        existing.averagePrice = totalValue / existing.totalRawUsdcSize;
        existing.lastTradeTime = now;

        const tokens = existing.averagePrice > 0 ? existing.totalScaledUsdcSize / existing.averagePrice : 0;
        Logger.info(`   📦 Buffer updated: ${existing.trades.length} trades | ${tokens.toFixed(2)}/${MIN_TOKENS_FOR_ORDER} tokens | ${existing.slug || existing.asset.slice(0, 10)}`);
    } else {
        const tokens = trade.price > 0 ? scaledSize / trade.price : 0;
        Logger.info(`   📦 New buffer: ${tokens.toFixed(2)}/${MIN_TOKENS_FOR_ORDER} tokens | ${trade.slug || trade.asset.slice(0, 10)}`);

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

    // Log total buffer state
    Logger.info(`   📊 Total buffers: ${tradeAggregationBuffer.size}`);
};

/**
 * Get aggregations that have >= 5 tokens (ready for execution attempt)
 */
const getReadyAggregations = (): AggregatedTrade[] => {
    const ready: AggregatedTrade[] = [];

    if (tradeAggregationBuffer.size > 0) {
        Logger.info(`🔍 Checking ${tradeAggregationBuffer.size} buffer(s) for readiness...`);
    }

    for (const [key, agg] of tradeAggregationBuffer.entries()) {
        const totalTokens = agg.averagePrice > 0 ? agg.totalScaledUsdcSize / agg.averagePrice : 0;

        if (totalTokens >= MIN_TOKENS_FOR_ORDER) {
            Logger.info(`   ✅ READY: ${agg.trades.length} trades | ${totalTokens.toFixed(2)} tokens @ $${agg.averagePrice.toFixed(4)} | ${agg.slug || agg.asset.slice(0, 10)}`);
            ready.push(agg);
            tradeAggregationBuffer.delete(key);
        } else {
            Logger.info(`   ⏳ Waiting: ${totalTokens.toFixed(2)}/${MIN_TOKENS_FOR_ORDER} tokens | ${agg.slug || agg.asset.slice(0, 10)}`);
        }
    }

    return ready;
};

/**
 * Execute a single trade (non-aggregated)
 */
const doTrading = async (clobClient: ClobClient, trades: TradeWithUser[]) => {
    for (const trade of trades) {
        try {
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
                (pos: UserPositionInterface) => pos.conditionId === trade.conditionId
            );
            const user_position = user_positions.find(
                (pos: UserPositionInterface) => pos.conditionId === trade.conditionId
            );
            const my_balance = await getMyBalance(PROXY_WALLET);
            const user_balance = user_positions.reduce((total, pos) => total + (pos.currentValue || 0), 0);

            Logger.balance(my_balance, user_balance, trade.userAddress);

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
            const UserActivity = getUserActivityModel(trade.userAddress);
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
};

/**
 * Execute aggregated trades WITH arb check at execution time
 */
const doAggregatedTrading = async (clobClient: ClobClient, aggregatedTrades: AggregatedTrade[]) => {
    for (const agg of aggregatedTrades) {
        try {
            const totalTokens = agg.averagePrice > 0 ? agg.totalScaledUsdcSize / agg.averagePrice : 0;

            Logger.header(`📊 EXECUTING AGGREGATED TRADE`);
            Logger.info(`Market: ${agg.slug || agg.conditionId}`);
            Logger.info(`Asset: ${agg.asset.slice(0, 10)}...`);
            Logger.info(`${agg.trades.length} trades | $${agg.totalScaledUsdcSize.toFixed(2)} | ${totalTokens.toFixed(2)} tokens @ $${agg.averagePrice.toFixed(4)}`);

            // =====================================================
            // ARB CHECK - RIGHT BEFORE EXECUTION
            // Discovers opposite side from what we've already executed for this conditionId
            // =====================================================
            Logger.info(`🎯 Checking arb constraints for ${agg.slug || agg.conditionId.slice(0, 10)}...`);
            const arbCheck = checkArbBeforeExecution(
                agg.conditionId,
                agg.asset,
                totalTokens,
                agg.averagePrice,
                agg.slug
            );

            Logger.info(`🎯 Result: ${arbCheck.reason}`);

            if (!arbCheck.ok) {
                Logger.warning(`⛔ Arb check FAILED - skipping execution`);
                // Mark trades as processed but not executed
                for (const trade of agg.trades) {
                    const UserActivity = getUserActivityModel(trade.userAddress);
                    await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                }
                Logger.separator();
                continue;
            }

            // Mark trades as being processed
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
                (pos: UserPositionInterface) => pos.conditionId === agg.conditionId
            );
            const user_position = user_positions.find(
                (pos: UserPositionInterface) => pos.conditionId === agg.conditionId
            );
            const my_balance = await getMyBalance(PROXY_WALLET);
            const user_balance = user_positions.reduce((total, pos) => total + (pos.currentValue || 0), 0);

            Logger.balance(my_balance, user_balance, agg.userAddress);

            const syntheticTrade: UserActivityInterface = {
                ...agg.trades[0],
                usdcSize: agg.totalRawUsdcSize,
                price: agg.averagePrice,
                side: agg.side as 'BUY' | 'SELL',
            };

            await postOrder(
                clobClient,
                agg.side === 'BUY' ? 'buy' : 'sell',
                my_position,
                user_position,
                syntheticTrade,
                my_balance,
                user_balance,
                agg.userAddress,
                agg.totalScaledUsdcSize
            );

            // Record the execution for future arb checks
            recordExecution(agg.conditionId, agg.asset, totalTokens, agg.totalScaledUsdcSize, agg.slug);

            Logger.separator();
        } catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            Logger.warning(`⚠️ Error executing aggregated trade: ${errMsg}`);
            for (const trade of agg.trades) {
                const UserActivity = getUserActivityModel(trade.userAddress);
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            }
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
};

let isRunning = true;

export const stopTradeExecutor = () => {
    isRunning = false;
    Logger.info('Trade executor shutdown requested...');
};

const tradeExecutor = async (clobClient: ClobClient) => {
    Logger.success(`Trade executor ready for ${USER_ADDRESSES.length} trader(s)`);
    if (ALLOWED_MARKET_KEYWORDS.length > 0) {
        const filterDesc = ALLOWED_MARKET_KEYWORDS.map(kw => kw.join('+')).join(' OR ');
        Logger.info(`🎯 Market filter: ${filterDesc}`);
    }
    if (TRADE_AGGREGATION_ENABLED) {
        Logger.info(`📊 Aggregation: buffer until >= ${MIN_TOKENS_FOR_ORDER} tokens`);
        Logger.info(`🎯 Arb check at EXECUTION time: cost basis < ${ARB_MAX_COST_BASIS * 100}¢, imbalance < ${ARB_MAX_IMBALANCE * 100}%`);
    }

    let lastCheck = Date.now();
    while (isRunning) {
        const allTrades = await readTempTrades();

        // Debug: log all trades found with FULL conditionId and asset
        if (allTrades.length > 0) {
            Logger.info(`📡 Found ${allTrades.length} unprocessed trade(s):`);
            for (const t of allTrades) {
                Logger.info(`   → ${t.side} | ${t.slug || 'no-slug'}`);
                Logger.info(`     conditionId: ${t.conditionId}`);
                Logger.info(`     asset: ${t.asset}`);
                Logger.info(`     $${t.usdcSize.toFixed(2)} @ $${t.price.toFixed(4)}`);
            }
        }

        // Filter by allowed markets
        const trades: TradeWithUser[] = [];
        const filteredOut: TradeWithUser[] = [];

        for (const trade of allTrades) {
            if (isAllowedMarket(trade)) {
                trades.push(trade);
            } else {
                filteredOut.push(trade);
            }
        }

        // Mark filtered-out trades as processed
        if (filteredOut.length > 0) {
            for (const trade of filteredOut) {
                const UserActivity = getUserActivityModel(trade.userAddress);
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                Logger.info(`   🚫 Filtered: ${trade.slug || trade.eventSlug || 'unknown'}`);
            }
            Logger.info(`🚫 Filtered ${filteredOut.length} trade(s) - didn't match: ${ALLOWED_MARKET_KEYWORDS.map(kw => kw.join('+')).join(' OR ')}`);
        }

        if (TRADE_AGGREGATION_ENABLED) {
            try {
                if (trades.length > 0) {
                    Logger.clearLine();
                    Logger.info(`📥 ${trades.length} new trade(s) detected`);

                    const my_balance = await getMyBalance(PROXY_WALLET);
                    const my_positions: UserPositionInterface[] = await fetchData(
                        `https://data-api.polymarket.com/positions?user=${PROXY_WALLET}`
                    );

                    for (const trade of trades) {
                        const my_position = my_positions.find(pos => pos.conditionId === trade.conditionId);
                        const currentPositionValue = my_position ? my_position.size * my_position.avgPrice : 0;

                        const orderCalc = calculateOrderSize(
                            COPY_STRATEGY_CONFIG,
                            trade.usdcSize,
                            my_balance,
                            currentPositionValue
                        );

                        trade.scaledUsdcSize = orderCalc.finalAmount;

                        if (trade.side === 'BUY') {
                            const scaledTokens = orderCalc.finalAmount / trade.price;
                            Logger.info(`📥 Buffer: $${trade.usdcSize.toFixed(2)} → $${orderCalc.finalAmount.toFixed(2)} / ${scaledTokens.toFixed(2)} tokens | ${trade.slug || trade.asset.slice(0, 10)}`);

                            // Mark as being processed
                            const UserActivity = getUserActivityModel(trade.userAddress);
                            await UserActivity.updateOne({ _id: trade._id }, { $set: { botExcutedTime: 1 } });

                            // Add to buffer - NO REJECTION
                            addToAggregationBuffer(trade);
                        } else {
                            // SELL trades execute immediately
                            Logger.header(`⚡ SELL TRADE`);
                            await doTrading(clobClient, [trade]);
                        }
                    }
                    lastCheck = Date.now();
                }

                // Check for ready aggregations
                const readyAggs = getReadyAggregations();
                if (readyAggs.length > 0) {
                    Logger.clearLine();
                    Logger.header(`⚡ ${readyAggs.length} AGGREGATION(S) READY`);
                    await doAggregatedTrading(clobClient, readyAggs);
                    lastCheck = Date.now();
                }
            } catch (err) {
                const errMsg = err instanceof Error ? err.message : String(err);
                if (errMsg.includes('timeout') || errMsg.includes('ETIMEDOUT') || errMsg.includes('network')) {
                    Logger.warning(`⚠️ Network timeout - will retry`);
                    await new Promise(resolve => setTimeout(resolve, 5000));
                } else {
                    Logger.warning(`⚠️ Error: ${errMsg}`);
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }

            // Update waiting message
            if (trades.length === 0 && Date.now() - lastCheck > 300) {
                const bufferCount = tradeAggregationBuffer.size;
                const marketCount = marketTrackers.size;

                if (bufferCount > 0 || marketCount > 0) {
                    let bufferTokens = 0;
                    for (const agg of tradeAggregationBuffer.values()) {
                        if (agg.averagePrice > 0) {
                            bufferTokens += agg.totalScaledUsdcSize / agg.averagePrice;
                        }
                    }

                    // Show market tracker summary
                    let marketsWithBothSides = 0;
                    for (const tracker of marketTrackers.values()) {
                        if (tracker.assets.size === 2) marketsWithBothSides++;
                    }

                    Logger.waiting(
                        USER_ADDRESSES.length,
                        `${bufferCount} buffered (${bufferTokens.toFixed(1)} tokens) | ${marketCount} markets (${marketsWithBothSides} paired)`
                    );

                    // Dump full state every 10 seconds for debugging
                    if (Date.now() % 10000 < 500) {
                        Logger.info(`\n📊 === FULL STATE DUMP ===`);
                        Logger.info(`BUFFERS (${tradeAggregationBuffer.size}):`);
                        for (const [key, agg] of tradeAggregationBuffer.entries()) {
                            const tokens = agg.averagePrice > 0 ? agg.totalScaledUsdcSize / agg.averagePrice : 0;
                            Logger.info(`  ${key.slice(0, 50)}...`);
                            Logger.info(`    ${tokens.toFixed(2)} tokens, conditionId=${agg.conditionId.slice(0, 20)}...`);
                        }
                        Logger.info(`MARKET TRACKERS (${marketTrackers.size}):`);
                        for (const [condId, tracker] of marketTrackers.entries()) {
                            Logger.info(`  ${tracker.slug || condId.slice(0, 20)}... (${tracker.assets.size} sides)`);
                            for (const [asset, data] of tracker.assets.entries()) {
                                Logger.info(`    ${asset.slice(0, 16)}...: ${data.quantity.toFixed(2)} tokens`);
                            }
                        }
                        Logger.info(`=========================\n`);
                    }
                } else {
                    Logger.waiting(USER_ADDRESSES.length);
                }
                lastCheck = Date.now();
            }
        } else {
            // Non-aggregation mode
            if (trades.length > 0) {
                Logger.clearLine();
                Logger.header(`⚡ ${trades.length} NEW TRADE(S)`);
                await doTrading(clobClient, trades);
                lastCheck = Date.now();
            } else if (Date.now() - lastCheck > 300) {
                Logger.waiting(USER_ADDRESSES.length);
                lastCheck = Date.now();
            }
        }

        if (!isRunning) break;
        await new Promise(resolve => setTimeout(resolve, 300));
    }

    Logger.info('Trade executor stopped');
};

export default tradeExecutor;
