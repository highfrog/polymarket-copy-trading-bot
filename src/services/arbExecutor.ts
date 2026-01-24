import { ClobClient, Side, OrderType } from '@polymarket/clob-client';
import { ENV } from '../config/env';
import getMyBalance from '../utils/getMyBalance';
import Logger from '../utils/logger';

// Precision helpers (same as postOrder.ts)
const roundToDecimals = (value: number, decimals: number): number => {
    const factor = Math.pow(10, decimals);
    const rounded = Math.floor(value * factor) / factor;
    return parseFloat(rounded.toFixed(decimals));
};
const roundPrice = (price: number): number => roundToDecimals(price, 2);

const PROXY_WALLET = ENV.PROXY_WALLET;
const CLOB_HTTP_URL = ENV.CLOB_HTTP_URL;

// ============================================================================
// ARB BOT CONFIGURATION
// ============================================================================
const ARB_CONFIG = {
    // DRY RUN MODE - set to true to only log, no actual trades
    dryRun: true,

    // Coins to trade
    coins: ['btc', 'eth'],

    // Maximum combined cost basis to consider (YES + NO < this = profit)
    maxCostBasis: 0.95,

    // Minimum profit per share to act on (e.g., 0.03 = 3 cents per share)
    minProfitPerShare: 0.02,

    // Order size in USD per side
    orderSizeUsd: 5.00,

    // Minimum tokens per order (Polymarket requires >= 5 for GTC)
    minTokens: 5,

    // Maximum position imbalance (0.25 = 25%)
    maxImbalance: 0.30,

    // How often to check for opportunities (ms)
    checkIntervalMs: 5000,
};

// ============================================================================
// TYPES
// ============================================================================
interface Market {
    conditionId?: string;
    condition_id?: string;  // Gamma API format
    questionId?: string;
    slug?: string;
    question?: string;
    outcomes?: string[];
    tokens?: TokenInfo[];
    active?: boolean;
    closed?: boolean;
    endDate?: string;
}

// Helper to get conditionId from either format
function getConditionId(market: Market): string {
    return market.conditionId || market.condition_id || '';
}

interface TokenInfo {
    token_id: string;
    outcome: string;
    price: number;
}

interface OrderBookLevel {
    price: string;
    size: string;
}

interface OrderBook {
    bids: OrderBookLevel[];
    asks: OrderBookLevel[];
    asset_id: string;
    timestamp: string;
}

interface ArbOpportunity {
    market: Market;
    yesToken: TokenInfo;
    noToken: TokenInfo;
    yesBestAsk: number;
    noBestAsk: number;
    combinedCost: number;
    profitPerShare: number;
    maxShares: number;
}

interface Position {
    conditionId: string;
    slug: string;
    yesTokenId: string;
    noTokenId: string;
    yesQty: number;
    noQty: number;
    yesCost: number;
    noCost: number;
    yesAvgPrice: number;
    noAvgPrice: number;
}

// ============================================================================
// STATE
// ============================================================================
const positions: Map<string, Position> = new Map();
let isRunning = true;

// ============================================================================
// MARKET DISCOVERY
// ============================================================================

/**
 * Calculate the current 15-minute window start timestamp in EASTERN TIME
 * Windows are at :00, :15, :30, :45 of each hour ET
 */
function getCurrent15mWindowTimestamp(): number {
    const now = new Date();

    // Get current time components in ET using Intl API
    const etString = now.toLocaleString('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
    });

    // Parse: "01/22/2026, 14:30:00"
    const match = etString.match(/(\d+)\/(\d+)\/(\d+),?\s+(\d+):(\d+):(\d+)/);
    if (!match) throw new Error(`Failed to parse ET time: ${etString}`);

    const [, monthStr, dayStr, yearStr, hourStr, minuteStr] = match;
    const year = parseInt(yearStr);
    const month = parseInt(monthStr);
    const day = parseInt(dayStr);
    const hour = parseInt(hourStr);
    const minute = parseInt(minuteStr);

    // Round to 15-minute window
    const roundedMinute = Math.floor(minute / 15) * 15;

    // Calculate ET offset from UTC (ET is behind UTC by 4-5 hours)
    const utcHour = now.getUTCHours();
    let offsetHours = hour - utcHour;
    if (offsetHours > 12) offsetHours -= 24;
    if (offsetHours < -12) offsetHours += 24;

    // Create timestamp: treat ET time as UTC, then adjust by offset
    const asIfUtc = Date.UTC(year, month - 1, day, hour, roundedMinute, 0, 0);
    const actualUtc = asIfUtc - (offsetHours * 60 * 60 * 1000);

    return Math.floor(actualUtc / 1000);
}

/**
 * Generate slug for a 15m updown market
 * Pattern: {coin}-updown-15m-{unix_timestamp_ET}
 */
function generate15mSlug(coin: string, timestamp?: number): string {
    const ts = timestamp || getCurrent15mWindowTimestamp();
    return `${coin.toLowerCase()}-updown-15m-${ts}`;
}

/**
 * Fetch event/market data from Gamma API by slug
 * Endpoint: https://gamma-api.polymarket.com/events/slug/{slug}
 * Returns the event's markets array (usually contains one market for binary events)
 */
async function fetchMarketsBySlug(slug: string): Promise<Market[]> {
    try {
        const url = `https://gamma-api.polymarket.com/events/slug/${slug}`;
        const resp = await fetch(url);

        if (!resp.ok) {
            return [];
        }

        const event = await resp.json() as Record<string, unknown>;
        const markets = event.markets as Market[] | undefined;

        return markets || [];
    } catch (err) {
        return [];
    }
}

/**
 * Find active 15-minute up/down markets
 * Uses Gamma API: /events/slug/{coin}-updown-15m-{timestamp_ET}
 */
async function find15mUpDownMarkets(coins: string[] = ['btc', 'eth'], clobClient?: ClobClient): Promise<Market[]> {
    const markets: Market[] = [];
    const currentWindowTs = getCurrent15mWindowTimestamp();

    // Try previous, current, and next windows for each coin
    const windowOffsets = [-15 * 60, 0, 15 * 60];

    for (const coin of coins) {
        for (const offset of windowOffsets) {
            const ts = currentWindowTs + offset;
            const slug = generate15mSlug(coin, ts);
            const eventMarkets = await fetchMarketsBySlug(slug);

            for (const market of eventMarkets) {
                markets.push(market);
            }
        }
    }

    return markets;
}

// Store clobClient reference for market discovery
let _clobClient: ClobClient | null = null;

// Wrapper using config
async function discoverMarkets(): Promise<Market[]> {
    return find15mUpDownMarkets(ARB_CONFIG.coins, _clobClient || undefined);
}

/**
 * Get token info for a market (YES and NO tokens)
 */
async function getMarketTokens(market: Market): Promise<{ yes: TokenInfo; no: TokenInfo } | null> {
    try {
        // Gamma API uses clobTokenIds array [yesTokenId, noTokenId]
        const marketAny = market as Record<string, unknown>;
        // Parse clobTokenIds - might be array or JSON string
        let clobTokenIds: string[] | undefined;
        const rawTokenIds = marketAny.clobTokenIds;
        if (Array.isArray(rawTokenIds)) {
            clobTokenIds = rawTokenIds;
        } else if (typeof rawTokenIds === 'string') {
            try {
                clobTokenIds = JSON.parse(rawTokenIds);
            } catch { /* leave undefined */ }
        }

        if (clobTokenIds && clobTokenIds.length >= 2) {
            // Gamma format: clobTokenIds[0] = YES, clobTokenIds[1] = NO
            let yesPrice = 0.5, noPrice = 0.5;

            // Parse outcomePrices - might be array or JSON string like "[\"0.405\", \"0.595\"]"
            const rawPrices = marketAny.outcomePrices;
            if (rawPrices) {
                let pricesArr: number[] = [];
                if (Array.isArray(rawPrices)) {
                    pricesArr = rawPrices.map(p => parseFloat(String(p)));
                } else if (typeof rawPrices === 'string') {
                    try {
                        // Try JSON parse first (handles "[\"0.405\", \"0.595\"]")
                        const parsed = JSON.parse(rawPrices);
                        if (Array.isArray(parsed)) {
                            pricesArr = parsed.map(p => parseFloat(String(p)));
                        }
                    } catch {
                        // Fallback: comma-separated string
                        const pricesStr = rawPrices.replace(/[\[\]\"]/g, '');
                        pricesArr = pricesStr.split(',').map(p => parseFloat(p.trim()));
                    }
                }
                if (pricesArr.length >= 2 && !isNaN(pricesArr[0]) && !isNaN(pricesArr[1])) {
                    yesPrice = pricesArr[0];
                    noPrice = pricesArr[1];
                }
            }

            // Parse outcomes - might be array or JSON string
            let outcomesList: string[] = ['Yes', 'No'];
            const rawOutcomes = marketAny.outcomes;
            if (Array.isArray(rawOutcomes)) {
                outcomesList = rawOutcomes;
            } else if (typeof rawOutcomes === 'string') {
                try {
                    outcomesList = JSON.parse(rawOutcomes);
                } catch { /* use default */ }
            }

            const result = {
                yes: {
                    token_id: clobTokenIds[0],
                    outcome: outcomesList[0] || 'Yes',
                    price: yesPrice,
                },
                no: {
                    token_id: clobTokenIds[1],
                    outcome: outcomesList[1] || 'No',
                    price: noPrice,
                },
            };
            return result;
        }

        // Fallback: try tokens array (CLOB API format)
        if (!market.tokens || market.tokens.length < 2) {
            const conditionId = market.conditionId || (marketAny.condition_id as string);
            if (!conditionId) return null;

            const response = await fetch(`${CLOB_HTTP_URL}/markets/${conditionId}`);
            if (!response.ok) return null;

            const fullMarket = await response.json();
            if (!fullMarket.tokens || fullMarket.tokens.length < 2) return null;

            market.tokens = fullMarket.tokens;
        }

        const yesToken = market.tokens.find(t => t.outcome?.toLowerCase() === 'yes');
        const noToken = market.tokens.find(t => t.outcome?.toLowerCase() === 'no');

        if (!yesToken || !noToken) {
            // Maybe outcomes are "Up"/"Down" or indexed
            const token0 = market.tokens[0];
            const token1 = market.tokens[1];
            return { yes: token0, no: token1 };
        }

        return { yes: yesToken, no: noToken };
    } catch (err) {
        Logger.warning(`Error getting tokens for ${market.slug}: ${err instanceof Error ? err.message : String(err)}`);
        return null;
    }
}

// ============================================================================
// ORDER BOOK
// ============================================================================

/**
 * Fetch order book for a token
 */
async function getOrderBook(tokenId: string): Promise<OrderBook | null> {
    try {
        const response = await fetch(`${CLOB_HTTP_URL}/book?token_id=${tokenId}`);
        if (!response.ok) return null; // 404 expected for resolved markets

        return await response.json();
    } catch (err) {
        return null;
    }
}

/**
 * Get best ask price from order book (lowest ask)
 */
function getBestAsk(book: OrderBook): number | null {
    if (!book.asks || book.asks.length === 0) return null;

    // Sort asks by price ascending to find the lowest (best) ask
    const sortedAsks = [...book.asks].sort((a, b) => parseFloat(a.price) - parseFloat(b.price));
    return parseFloat(sortedAsks[0].price);
}

/**
 * Get best bid price from order book (highest bid)
 */
function getBestBid(book: OrderBook): number | null {
    if (!book.bids || book.bids.length === 0) return null;

    // Sort bids by price descending to find the highest (best) bid
    const sortedBids = [...book.bids].sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
    return parseFloat(sortedBids[0].price);
}

/**
 * Get available liquidity at or below a price
 */
function getLiquidityAtPrice(book: OrderBook, maxPrice: number): { price: number; size: number }[] {
    const levels: { price: number; size: number }[] = [];

    for (const ask of book.asks) {
        const price = parseFloat(ask.price);
        if (price > maxPrice) break;

        levels.push({
            price,
            size: parseFloat(ask.size),
        });
    }

    return levels;
}

// ============================================================================
// ARB DETECTION
// ============================================================================

/**
 * Check a market for arb opportunity
 */
async function checkArbOpportunity(market: Market): Promise<ArbOpportunity | null> {
    const mAny = market as Record<string, unknown>;
    const marketName = (mAny.question || mAny.slug || 'unknown') as string;

    // Extract coin and time from market name for cleaner display
    let displayName = marketName;
    if (marketName.toLowerCase().includes('bitcoin')) {
        const timeMatch = marketName.match(/(\d+:\d+[AP]M-\d+:\d+[AP]M)/);
        displayName = `BTC-15m ${timeMatch ? timeMatch[1] : ''}`;
    } else if (marketName.toLowerCase().includes('ethereum')) {
        const timeMatch = marketName.match(/(\d+:\d+[AP]M-\d+:\d+[AP]M)/);
        displayName = `ETH-15m ${timeMatch ? timeMatch[1] : ''}`;
    }

    const tokens = await getMarketTokens(market);
    if (!tokens) return null;

    const [yesBook, noBook] = await Promise.all([
        getOrderBook(tokens.yes.token_id),
        getOrderBook(tokens.no.token_id),
    ]);

    // If order books unavailable (resolved market), show Gamma prices
    if (!yesBook || !noBook) {
        const gammaCombined = tokens.yes.price + tokens.no.price;
        Logger.info(`${displayName.padEnd(24)} ${tokens.yes.outcome}=$${tokens.yes.price.toFixed(2)} ${tokens.no.outcome}=$${tokens.no.price.toFixed(2)} → $${gammaCombined.toFixed(2)} (final)`);
        return null;
    }

    const yesBestAsk = getBestAsk(yesBook);
    const noBestAsk = getBestAsk(noBook);
    const yesBestBid = getBestBid(yesBook);
    const noBestBid = getBestBid(noBook);

    if (yesBestAsk === null || noBestAsk === null) return null;

    const combinedCost = yesBestAsk + noBestAsk;
    const profitPerShare = 1.0 - combinedCost;

    Logger.info(`${displayName.padEnd(24)} ${tokens.yes.outcome}=$${yesBestAsk.toFixed(2)} ${tokens.no.outcome}=$${noBestAsk.toFixed(2)} → $${combinedCost.toFixed(2)}`);

    if (combinedCost >= ARB_CONFIG.maxCostBasis) {
        return null;
    }

    if (profitPerShare < ARB_CONFIG.minProfitPerShare) {
        return null;
    }

    // Calculate max shares we can buy at these prices
    const yesLiquidity = getLiquidityAtPrice(yesBook, yesBestAsk + 0.01);
    const noLiquidity = getLiquidityAtPrice(noBook, noBestAsk + 0.01);

    const yesAvailable = yesLiquidity.reduce((sum, l) => sum + l.size, 0);
    const noAvailable = noLiquidity.reduce((sum, l) => sum + l.size, 0);
    const maxShares = Math.min(yesAvailable, noAvailable);

    return {
        market,
        yesToken: tokens.yes,
        noToken: tokens.no,
        yesBestAsk,
        noBestAsk,
        combinedCost,
        profitPerShare,
        maxShares,
    };
}

// ============================================================================
// POSITION MANAGEMENT
// ============================================================================

/**
 * Get or create position tracker for a market
 */
function getPosition(conditionId: string, market?: Market, tokens?: { yes: TokenInfo; no: TokenInfo }): Position {
    let pos = positions.get(conditionId);
    if (!pos && market && tokens) {
        pos = {
            conditionId,
            slug: market.slug,
            yesTokenId: tokens.yes.token_id,
            noTokenId: tokens.no.token_id,
            yesQty: 0,
            noQty: 0,
            yesCost: 0,
            noCost: 0,
            yesAvgPrice: 0,
            noAvgPrice: 0,
        };
        positions.set(conditionId, pos);
    }
    return pos!;
}

/**
 * Record a fill
 */
function recordFill(conditionId: string, side: 'yes' | 'no', qty: number, cost: number): void {
    const pos = positions.get(conditionId);
    if (!pos) return;

    if (side === 'yes') {
        pos.yesQty += qty;
        pos.yesCost += cost;
        pos.yesAvgPrice = pos.yesCost / pos.yesQty;
    } else {
        pos.noQty += qty;
        pos.noCost += cost;
        pos.noAvgPrice = pos.noCost / pos.noQty;
    }
}

/**
 * Check if we should buy more of a side (balance check)
 */
function shouldBuySide(pos: Position, side: 'yes' | 'no', additionalQty: number): boolean {
    const currentYes = pos.yesQty;
    const currentNo = pos.noQty;

    const newYes = side === 'yes' ? currentYes + additionalQty : currentYes;
    const newNo = side === 'no' ? currentNo + additionalQty : currentNo;

    const total = newYes + newNo;
    if (total === 0) return true;

    const imbalance = Math.abs(newYes - newNo) / total;

    // If this trade would worsen imbalance beyond threshold, skip
    const currentImbalance = total > 0 ? Math.abs(currentYes - currentNo) / (currentYes + currentNo) : 0;

    if (imbalance > ARB_CONFIG.maxImbalance && imbalance > currentImbalance) {
        return false;
    }

    return true;
}

// ============================================================================
// ORDER EXECUTION
// ============================================================================

/**
 * Place a GTC limit buy order (or simulate in dry-run mode)
 */
async function placeBuyOrder(
    clobClient: ClobClient,
    tokenId: string,
    price: number,
    size: number
): Promise<boolean> {
    try {
        // Round to valid precision: tokens 2 decimals, price 2 decimals
        const priceCents = Math.round(price * 100);
        const priceStr = (priceCents / 100).toFixed(2);
        const finalPrice = parseFloat(priceStr);

        const tokensCents = Math.floor(size * 100);
        const tokensStr = (tokensCents / 100).toFixed(2);
        const finalTokens = parseFloat(tokensStr);

        if (finalTokens < ARB_CONFIG.minTokens) {
            Logger.info(`   Skip: ${finalTokens} tokens < ${ARB_CONFIG.minTokens} minimum`);
            return false;
        }

        // DRY RUN MODE - just log, don't execute
        if (ARB_CONFIG.dryRun) {
            Logger.info(`   🧪 [DRY RUN] Would place: ${tokensStr} tokens @ $${priceStr} ($${(finalTokens * finalPrice).toFixed(2)})`);
            return true; // Simulate success
        }

        Logger.info(`   Placing GTC limit: ${tokensStr} tokens @ $${priceStr}`);

        const order_args = {
            side: Side.BUY,
            tokenID: tokenId,
            size: finalTokens,
            price: finalPrice,
        };

        const signedOrder = await clobClient.createOrder(order_args, { tickSize: "0.01" as const });
        const resp = await clobClient.postOrder(signedOrder, OrderType.GTC);

        if (resp.success === true) {
            Logger.success(`   Order placed successfully`);
            return true;
        } else {
            const errorMsg = typeof resp === 'object' && resp !== null
                ? (resp as Record<string, unknown>).errorMsg || (resp as Record<string, unknown>).error || 'Unknown error'
                : 'Unknown error';
            Logger.warning(`   Order failed: ${errorMsg}`);
            return false;
        }
    } catch (err) {
        Logger.warning(`   Order error: ${err instanceof Error ? err.message : String(err)}`);
        return false;
    }
}

/**
 * Execute arb opportunity - buy both sides
 */
async function executeArb(clobClient: ClobClient, opp: ArbOpportunity): Promise<void> {
    Logger.header(`⚡ ARB OPPORTUNITY`);
    Logger.info(`Market: ${opp.market.slug}`);
    Logger.info(`YES @ $${opp.yesBestAsk.toFixed(4)} + NO @ $${opp.noBestAsk.toFixed(4)} = $${opp.combinedCost.toFixed(4)}`);
    Logger.info(`Profit per share: $${opp.profitPerShare.toFixed(4)} (${(opp.profitPerShare * 100).toFixed(2)}%)`);
    Logger.info(`Liquidity: ${opp.maxShares.toFixed(2)} shares available`);

    // Get current balance
    const balance = await getMyBalance(PROXY_WALLET);
    Logger.info(`Balance: $${balance.toFixed(2)}`);

    // Calculate order size
    const maxUsdPerSide = Math.min(ARB_CONFIG.orderSizeUsd, balance / 2);
    const yesQty = maxUsdPerSide / opp.yesBestAsk;
    const noQty = maxUsdPerSide / opp.noBestAsk;

    // Use the smaller quantity to stay balanced
    const targetQty = Math.min(yesQty, noQty, opp.maxShares);

    if (targetQty < ARB_CONFIG.minTokens) {
        Logger.warning(`Insufficient size: ${targetQty.toFixed(2)} < ${ARB_CONFIG.minTokens} tokens`);
        return;
    }

    // Get/create position tracker
    const tokens = await getMarketTokens(opp.market);
    if (!tokens) return;

    const pos = getPosition(getConditionId(opp.market), opp.market, tokens);

    // Check balance constraints
    const canBuyYes = shouldBuySide(pos, 'yes', targetQty);
    const canBuyNo = shouldBuySide(pos, 'no', targetQty);

    Logger.info(`\nExecuting arb (${targetQty.toFixed(2)} shares each side):`);

    // Place YES order
    if (canBuyYes) {
        Logger.info(`📗 YES side:`);
        const yesSuccess = await placeBuyOrder(clobClient, opp.yesToken.token_id, opp.yesBestAsk, targetQty);
        if (yesSuccess) {
            recordFill(getConditionId(opp.market), 'yes', targetQty, targetQty * opp.yesBestAsk);
        }
    } else {
        Logger.info(`📗 YES side: skipped (would worsen imbalance)`);
    }

    // Small delay between orders
    await new Promise(r => setTimeout(r, 100));

    // Place NO order
    if (canBuyNo) {
        Logger.info(`📕 NO side:`);
        const noSuccess = await placeBuyOrder(clobClient, opp.noToken.token_id, opp.noBestAsk, targetQty);
        if (noSuccess) {
            recordFill(getConditionId(opp.market), 'no', targetQty, targetQty * opp.noBestAsk);
        }
    } else {
        Logger.info(`📕 NO side: skipped (would worsen imbalance)`);
    }

    // Log position state
    Logger.info(`\n📊 Position after trade:`);
    Logger.info(`   YES: ${pos.yesQty.toFixed(2)} @ $${pos.yesAvgPrice.toFixed(4)}`);
    Logger.info(`   NO:  ${pos.noQty.toFixed(2)} @ $${pos.noAvgPrice.toFixed(4)}`);
    if (pos.yesQty > 0 && pos.noQty > 0) {
        const costBasis = pos.yesAvgPrice + pos.noAvgPrice;
        const lockedProfit = (1 - costBasis) * Math.min(pos.yesQty, pos.noQty);
        Logger.info(`   Cost basis: $${costBasis.toFixed(4)} | Locked profit: $${lockedProfit.toFixed(2)}`);
    }

    Logger.separator();
}

// ============================================================================
// MAIN LOOP
// ============================================================================

export const stopArbExecutor = () => {
    isRunning = false;
    Logger.info('Arb executor shutdown requested...');
};

const arbExecutor = async (clobClient: ClobClient) => {
    // Store reference for market discovery
    _clobClient = clobClient;

    Logger.header(`🤖 ARB BOT STARTING`);
    if (ARB_CONFIG.dryRun) {
        Logger.warning(`🧪 DRY RUN MODE - No actual trades will be placed`);
    }
    Logger.info(`Coins: ${ARB_CONFIG.coins.join(', ').toUpperCase()}`);
    Logger.info(`Max cost basis: $${ARB_CONFIG.maxCostBasis.toFixed(2)}`);
    Logger.info(`Min profit/share: $${ARB_CONFIG.minProfitPerShare.toFixed(4)}`);
    Logger.info(`Order size: $${ARB_CONFIG.orderSizeUsd.toFixed(2)} per side`);
    Logger.separator();

    let lastOpportunityCheck = 0;

    while (isRunning) {
        try {
            const now = Date.now();

            // Check for opportunities periodically
            if (now - lastOpportunityCheck >= ARB_CONFIG.checkIntervalMs) {
                // Discover markets
                const markets = await discoverMarkets();

                if (markets.length > 0) {
                    // Check each market for arb opportunity
                    for (const market of markets) {
                        const opp = await checkArbOpportunity(market);

                        if (opp) {
                            await executeArb(clobClient, opp);
                        }
                    }

                    // Show status
                    let totalLockedProfit = 0;
                    for (const pos of positions.values()) {
                        if (pos.yesQty > 0 && pos.noQty > 0) {
                            const costBasis = pos.yesAvgPrice + pos.noAvgPrice;
                            totalLockedProfit += (1 - costBasis) * Math.min(pos.yesQty, pos.noQty);
                        }
                    }

                    // Clean status line
                    const statusParts: string[] = [];
                    if (positions.size > 0) {
                        statusParts.push(`${positions.size} pos`);
                    }
                    if (totalLockedProfit > 0) {
                        statusParts.push(`$${totalLockedProfit.toFixed(2)} profit`);
                    }
                    const statusSuffix = statusParts.length > 0 ? ` | ${statusParts.join(' | ')}` : '';
                    Logger.info(`───────────────────────────────────────────${statusSuffix}`);
                }

                lastOpportunityCheck = now;
            }
        } catch (err) {
            Logger.warning(`Error in arb loop: ${err instanceof Error ? err.message : String(err)}`);
            await new Promise(r => setTimeout(r, 5000));
        }

        await new Promise(r => setTimeout(r, 500));
    }

    Logger.info('Arb executor stopped');
};

export default arbExecutor;
