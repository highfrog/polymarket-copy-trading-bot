import { ClobClient, Side, OrderType } from '@polymarket/clob-client';
import { ENV } from '../config/env';
import Logger from '../utils/logger';

// ============================================================================
// TYPES
// ============================================================================
interface Market {
    conditionId?: string;
    condition_id?: string;
    slug?: string;
    question?: string;
}

interface TokenInfo {
    token_id: string;
    outcome: string;
    price: number;
}

interface OrderBook {
    bids: { price: string; size: string }[];
    asks: { price: string; size: string }[];
}

interface WindowPosition {
    conditionId: string;
    marketSlug: string;
    coin: string;
    windowEnd: number;
    yesTokenId: string;
    noTokenId: string;
    initialSide: 'yes' | 'no' | null;
    initialQty: number;
    initialPrice: number;
    initialCost: number;
    oppositeSide: 'yes' | 'no' | null;
    oppositeQty: number;
    oppositePrice: number;
    oppositeCost: number;
    hedged: boolean;
    hedgeQty: number;
    hedgePrice: number;
    hedgeCost: number;
    resolved: boolean;
    pnl: number;
}

// ============================================================================
// STATE
// ============================================================================
const positions: Map<string, WindowPosition> = new Map();
let isRunning = true;
let _clobClient: ClobClient | null = null;

// Paper trading state
let paperBalance: number;
let paperStartingBalance: number;
let paperTotalPnl = 0;
let paperTrades = 0;
let paperWins = 0;
let paperLosses = 0;

const CLOB_HTTP_URL = ENV.CLOB_HTTP_URL;
const PROXY_WALLET = ENV.PROXY_WALLET;

// Config from ENV
const config = {
    dryRun: ENV.WINDOW_STRATEGY_DRY_RUN,
    startingBalance: ENV.WINDOW_STRATEGY_BALANCE,
    initialBuyAmount: ENV.WINDOW_STRATEGY_INITIAL_BUY,
    coins: ENV.WINDOW_STRATEGY_COINS,
    entryThreshold: ENV.WINDOW_STRATEGY_ENTRY_THRESHOLD,
    oppositeEntryThreshold: ENV.WINDOW_STRATEGY_OPPOSITE_THRESHOLD,
    combinedThresholdForOpposite: ENV.WINDOW_STRATEGY_COMBINED_THRESHOLD,
    hedgeMinutesBeforeEnd: ENV.WINDOW_STRATEGY_HEDGE_MINUTES,
    // Sliding hedge threshold
    hedgeStart: ENV.WINDOW_STRATEGY_HEDGE_START,      // 100¢ at 5 min left
    hedgeRampPerMin: ENV.WINDOW_STRATEGY_HEDGE_RAMP,  // +5¢ per minute
    hedgeMax: ENV.WINDOW_STRATEGY_HEDGE_MAX,          // Cap at 125¢
    checkIntervalMs: 3000,
    minTokens: 5,
};

// Calculate sliding hedge threshold based on time remaining
function getHedgeThreshold(minsLeft: number): number {
    // At hedgeMinutesBeforeEnd (5 min): threshold = hedgeStart (100¢)
    // Each minute closer to end: threshold increases by hedgeRampPerMin (5¢)
    // Capped at hedgeMax (125¢)
    const minutesIntoHedgeWindow = config.hedgeMinutesBeforeEnd - minsLeft;
    const threshold = config.hedgeStart + (minutesIntoHedgeWindow * config.hedgeRampPerMin);
    return Math.min(threshold, config.hedgeMax);
}

// ============================================================================
// MARKET DISCOVERY
// ============================================================================
function getCurrent15mWindowTimestamp(): number {
    const now = new Date();
    const etString = now.toLocaleString('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false,
    });

    const match = etString.match(/(\d+)\/(\d+)\/(\d+),?\s+(\d+):(\d+):(\d+)/);
    if (!match) throw new Error(`Failed to parse ET time: ${etString}`);

    const [, monthStr, dayStr, yearStr, hourStr, minuteStr] = match;
    const year = parseInt(yearStr);
    const month = parseInt(monthStr);
    const day = parseInt(dayStr);
    const hour = parseInt(hourStr);
    const minute = parseInt(minuteStr);

    const roundedMinute = Math.floor(minute / 15) * 15;
    const utcHour = now.getUTCHours();
    let offsetHours = hour - utcHour;
    if (offsetHours > 12) offsetHours -= 24;
    if (offsetHours < -12) offsetHours += 24;

    const asIfUtc = Date.UTC(year, month - 1, day, hour, roundedMinute, 0, 0);
    const actualUtc = asIfUtc - (offsetHours * 60 * 60 * 1000);
    return Math.floor(actualUtc / 1000);
}

function generate15mSlug(coin: string, timestamp: number): string {
    return `${coin.toLowerCase()}-updown-15m-${timestamp}`;
}

function formatWindowTime(windowTs: number): string {
    const startMs = windowTs * 1000;
    const endMs = startMs + 15 * 60 * 1000;

    const start = new Date(startMs);
    const end = new Date(endMs);

    const formatTime = (d: Date) => {
        return d.toLocaleTimeString('en-US', {
            timeZone: 'America/New_York',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true,
        });
    };

    return `${formatTime(start)}-${formatTime(end)}`;
}

async function fetchMarketsBySlug(slug: string): Promise<Market[]> {
    try {
        const resp = await fetch(`https://gamma-api.polymarket.com/events/slug/${slug}`);
        if (!resp.ok) return [];
        const event = await resp.json() as Record<string, unknown>;
        return (event.markets as Market[]) || [];
    } catch { return []; }
}

async function getMarketTokens(market: Market): Promise<{ yes: TokenInfo; no: TokenInfo } | null> {
    try {
        const mAny = market as Record<string, unknown>;
        let clobTokenIds: string[] | undefined;
        const raw = mAny.clobTokenIds;
        if (Array.isArray(raw)) clobTokenIds = raw;
        else if (typeof raw === 'string') try { clobTokenIds = JSON.parse(raw); } catch {}

        if (!clobTokenIds || clobTokenIds.length < 2) return null;

        let yesPrice = 0.5, noPrice = 0.5;
        const rawPrices = mAny.outcomePrices;
        if (rawPrices) {
            let arr: number[] = [];
            if (Array.isArray(rawPrices)) arr = rawPrices.map(p => parseFloat(String(p)));
            else if (typeof rawPrices === 'string') {
                try { arr = JSON.parse(rawPrices).map((p: string) => parseFloat(p)); } catch {}
            }
            if (arr.length >= 2) { yesPrice = arr[0]; noPrice = arr[1]; }
        }

        let outcomes = ['Yes', 'No'];
        const rawOut = mAny.outcomes;
        if (Array.isArray(rawOut)) outcomes = rawOut;
        else if (typeof rawOut === 'string') try { outcomes = JSON.parse(rawOut); } catch {}

        return {
            yes: { token_id: clobTokenIds[0], outcome: outcomes[0] || 'Yes', price: yesPrice },
            no: { token_id: clobTokenIds[1], outcome: outcomes[1] || 'No', price: noPrice },
        };
    } catch { return null; }
}

async function getOrderBook(tokenId: string): Promise<OrderBook | null> {
    try {
        const resp = await fetch(`${CLOB_HTTP_URL}/book?token_id=${tokenId}`);
        if (!resp.ok) return null;
        return await resp.json();
    } catch { return null; }
}

function getBestAsk(book: OrderBook): number | null {
    if (!book.asks?.length) return null;
    const sorted = [...book.asks].sort((a, b) => parseFloat(a.price) - parseFloat(b.price));
    return parseFloat(sorted[0].price);
}

function getBestBid(book: OrderBook): number | null {
    if (!book.bids?.length) return null;
    const sorted = [...book.bids].sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
    return parseFloat(sorted[0].price);
}

// ============================================================================
// OPTIMAL CALCULATIONS
// ============================================================================
function calculateOptimalOppositeAmount(pos: WindowPosition, oppositeAsk: number): { qty: number; cost: number } {
    const spread = 1 - (pos.initialPrice + oppositeAsk);
    let mult = 1.0;
    if (spread > 0.15) mult = 1.5;
    else if (spread > 0.10) mult = 1.0;
    else if (spread > 0.05) mult = 0.75;
    else mult = 0.5;

    const targetAmt = config.initialBuyAmount * mult;
    const qty = targetAmt / oppositeAsk;
    return { qty, cost: qty * oppositeAsk };
}

function calculateOptimalHedgeAmount(pos: WindowPosition, hedgeAsk: number): { qty: number; cost: number; side: 'yes' | 'no' } {
    const yesQty = (pos.initialSide === 'yes' ? pos.initialQty : 0) + (pos.oppositeSide === 'yes' ? pos.oppositeQty : 0);
    const noQty = (pos.initialSide === 'no' ? pos.initialQty : 0) + (pos.oppositeSide === 'no' ? pos.oppositeQty : 0);

    const hedgeSide: 'yes' | 'no' = yesQty < noQty ? 'yes' : 'no';
    const imbalance = Math.abs(yesQty - noQty);

    if (imbalance < 1) return { qty: 0, cost: 0, side: hedgeSide };

    const totalCost = pos.initialCost + pos.oppositeCost;
    const minQty = Math.min(yesQty, noQty);
    const maxQty = Math.max(yesQty, noQty);

    const worstWithout = minQty - totalCost;
    const worstWith = maxQty - (totalCost + imbalance * hedgeAsk);

    if (worstWith <= worstWithout) return { qty: 0, cost: 0, side: hedgeSide };

    return { qty: imbalance, cost: imbalance * hedgeAsk, side: hedgeSide };
}

// ============================================================================
// ORDER EXECUTION
// ============================================================================
async function executeOrder(
    tokenId: string,
    side: 'yes' | 'no',
    qty: number,
    price: number,
    action: string,
    marketSlug: string
): Promise<boolean> {
    const cost = qty * price;

    if (qty < config.minTokens) {
        Logger.info(`   Skip: ${qty.toFixed(2)} < ${config.minTokens} min tokens`);
        return false;
    }

    if (config.dryRun) {
        // Paper trade
        if (cost > paperBalance) {
            Logger.warning(`   [PAPER] Insufficient balance: $${cost.toFixed(2)} > $${paperBalance.toFixed(2)}`);
            return false;
        }
        paperBalance -= cost;
        paperTrades++;

        Logger.info(`   [PAPER] ${action}: ${qty.toFixed(2)} ${side.toUpperCase()} @ ${(price*100).toFixed(1)}¢ = $${cost.toFixed(2)}`);
        Logger.info(`   [PAPER] Balance: $${paperBalance.toFixed(2)} | Total P&L: ${paperTotalPnl >= 0 ? '+' : ''}$${paperTotalPnl.toFixed(2)}`);
        return true;
    }

    // Live trade
    try {
        const finalPrice = Math.round(price * 100) / 100;
        const finalQty = Math.floor(qty * 100) / 100;

        Logger.info(`   Placing order: ${finalQty} ${side.toUpperCase()} @ $${finalPrice.toFixed(2)}`);

        const signedOrder = await _clobClient!.createOrder({
            side: Side.BUY,
            tokenID: tokenId,
            size: finalQty,
            price: finalPrice,
        }, { tickSize: "0.01" as const });

        const resp = await _clobClient!.postOrder(signedOrder, OrderType.GTC);

        if (resp.success) {
            Logger.success(`   Order filled`);
            return true;
        }
        Logger.warning(`   Order failed: ${(resp as any).errorMsg || 'Unknown'}`);
        return false;
    } catch (err) {
        Logger.warning(`   Order error: ${err instanceof Error ? err.message : String(err)}`);
        return false;
    }
}

// ============================================================================
// STRATEGY LOGIC
// ============================================================================
async function processWindow(coin: string, market: Market, tokens: { yes: TokenInfo; no: TokenInfo }, windowTs: number): Promise<void> {
    const mAny = market as Record<string, unknown>;
    const conditionId = (market.conditionId || mAny.condition_id) as string;
    const slug = (mAny.slug || generate15mSlug(coin, windowTs)) as string;

    const [yesBook, noBook] = await Promise.all([
        getOrderBook(tokens.yes.token_id),
        getOrderBook(tokens.no.token_id),
    ]);

    if (!yesBook || !noBook) return;

    const yesAsk = getBestAsk(yesBook);
    const noAsk = getBestAsk(noBook);
    if (yesAsk === null || noAsk === null) return;

    const combined = yesAsk + noAsk;
    const now = Date.now();
    const windowEnd = (windowTs + 15 * 60) * 1000;
    const minsLeft = (windowEnd - now) / 60000;

    const windowTimeRange = formatWindowTime(windowTs);
    const display = `${coin.toUpperCase()}-15m ${windowTimeRange}`;
    const timeStr = minsLeft > 0 ? `${minsLeft.toFixed(1)}m left` : 'ENDED';

    let pos = positions.get(conditionId);

    // ==================== NO POSITION - LOOK FOR ENTRY ====================
    if (!pos) {
        const cheaperSide: 'yes' | 'no' = yesAsk <= noAsk ? 'yes' : 'no';
        const cheaperPrice = cheaperSide === 'yes' ? yesAsk : noAsk;
        const cheaperToken = cheaperSide === 'yes' ? tokens.yes.token_id : tokens.no.token_id;

        Logger.info(`${display} | Y=${(yesAsk*100).toFixed(1)}¢ N=${(noAsk*100).toFixed(1)}¢ Σ=${(combined*100).toFixed(0)}¢ | ${timeStr}`);

        if (cheaperPrice <= config.entryThreshold && minsLeft > config.hedgeMinutesBeforeEnd) {
            const qty = config.initialBuyAmount / cheaperPrice;
            const ok = await executeOrder(cheaperToken, cheaperSide, qty, cheaperPrice, 'ENTRY', slug);

            if (ok) {
                pos = {
                    conditionId, marketSlug: slug, coin, windowEnd,
                    yesTokenId: tokens.yes.token_id, noTokenId: tokens.no.token_id,
                    initialSide: cheaperSide, initialQty: qty, initialPrice: cheaperPrice, initialCost: qty * cheaperPrice,
                    oppositeSide: null, oppositeQty: 0, oppositePrice: 0, oppositeCost: 0,
                    hedged: false, hedgeQty: 0, hedgePrice: 0, hedgeCost: 0,
                    resolved: false, pnl: 0,
                };
                positions.set(conditionId, pos);
                Logger.success(`   ENTERED ${cheaperSide.toUpperCase()} @ ${(cheaperPrice*100).toFixed(1)}¢`);
            }
        }
        return;
    }

    // ==================== HAS POSITION ====================
    const oppSide: 'yes' | 'no' = pos.initialSide === 'yes' ? 'no' : 'yes';
    const oppAsk = oppSide === 'yes' ? yesAsk : noAsk;
    const oppToken = oppSide === 'yes' ? tokens.yes.token_id : tokens.no.token_id;

    const totalCost = pos.initialCost + pos.oppositeCost + pos.hedgeCost;
    const posStatus = pos.hedged ? '🔒LOCKED' : (pos.oppositeSide ? `+${pos.oppositeSide.toUpperCase()}@${(pos.oppositePrice*100).toFixed(0)}¢` : '');
    Logger.info(`${display} | Y=${(yesAsk*100).toFixed(1)}¢ N=${(noAsk*100).toFixed(1)}¢ Σ=${(combined*100).toFixed(0)}¢ | ${timeStr} | ${pos.initialSide?.toUpperCase()}@${(pos.initialPrice*100).toFixed(0)}¢ ${posStatus}`);

    // ==================== POSITION LOCKED - NO MORE ENTRIES ====================
    if (pos.hedged) {
        return; // Position is locked after hedge, no more trades
    }

    // ==================== OPPOSITE SIDE ENTRY ====================
    // Check: opposite side < 50¢ AND cost basis (initial + opposite) < 85¢
    const potentialCostBasis = pos.initialPrice + oppAsk;

    if (!pos.oppositeSide) {
        const oppMeetsThreshold = oppAsk <= config.oppositeEntryThreshold;
        const costBasisMeetsThreshold = potentialCostBasis < config.combinedThresholdForOpposite;

        if (!oppMeetsThreshold || !costBasisMeetsThreshold) {
            const reasons = [];
            if (!oppMeetsThreshold) reasons.push(`${oppSide.toUpperCase()}=${(oppAsk*100).toFixed(0)}¢ need<${(config.oppositeEntryThreshold*100).toFixed(0)}¢`);
            if (!costBasisMeetsThreshold) reasons.push(`CostBasis=${(potentialCostBasis*100).toFixed(0)}¢ (${(pos.initialPrice*100).toFixed(0)}+${(oppAsk*100).toFixed(0)}) need<${(config.combinedThresholdForOpposite*100).toFixed(0)}¢`);
            Logger.info(`   ⏳ Opposite wait: ${reasons.join(' & ')}`);
        }
    }

    if (!pos.oppositeSide && oppAsk <= config.oppositeEntryThreshold && potentialCostBasis < config.combinedThresholdForOpposite) {
        const { qty, cost } = calculateOptimalOppositeAmount(pos, oppAsk);
        const spread = 1 - potentialCostBasis; // Guaranteed profit per share

        if (qty >= config.minTokens) {
            Logger.info(`   🎯 OPPOSITE TRIGGER!`);
            Logger.info(`      Initial: ${pos.initialSide?.toUpperCase()}@${(pos.initialPrice*100).toFixed(1)}¢`);
            Logger.info(`      Opposite: ${oppSide.toUpperCase()}@${(oppAsk*100).toFixed(1)}¢`);
            Logger.info(`      Cost Basis: ${(potentialCostBasis*100).toFixed(1)}¢ → Spread: ${(spread*100).toFixed(1)}¢ per share`);

            const ok = await executeOrder(oppToken, oppSide, qty, oppAsk, 'OPPOSITE', slug);
            if (ok) {
                pos.oppositeSide = oppSide;
                pos.oppositeQty = qty;
                pos.oppositePrice = oppAsk;
                pos.oppositeCost = cost;
                Logger.success(`   ✅ OPPOSITE ${oppSide.toUpperCase()} @ ${(oppAsk*100).toFixed(1)}¢ | Locked ${(spread*100).toFixed(1)}¢/share profit`);
            }
        }
        return;
    }

    // ==================== HEDGE DECISION (LAST 5 MIN) ====================
    // Sliding threshold: starts at 100¢, increases 5¢/min, caps at 125¢
    // Always hedge if we can lock in under the threshold to cap losses
    if (minsLeft <= config.hedgeMinutesBeforeEnd && !pos.hedged) {
        const hedgeThreshold = getHedgeThreshold(minsLeft);

        // Calculate what our cost basis would be if we hedge now
        const { qty, cost, side } = calculateOptimalHedgeAmount(pos, oppSide === 'yes' ? yesAsk : noAsk);
        const hedgeAsk = side === 'yes' ? yesAsk : noAsk;
        const hedgeToken = side === 'yes' ? tokens.yes.token_id : tokens.no.token_id;

        // Current cost basis (what we've spent)
        const currentCostBasis = pos.initialPrice + (pos.oppositePrice || 0);
        // If we hedge, what would our total cost basis be per balanced share?
        const hedgeCostBasis = pos.initialPrice + (pos.oppositePrice || hedgeAsk);

        Logger.info(`   ⏰ Hedge window: ${minsLeft.toFixed(1)}m left | Threshold: ${(hedgeThreshold*100).toFixed(0)}¢ | Cost basis: ${(hedgeCostBasis*100).toFixed(0)}¢`);

        if (hedgeCostBasis < hedgeThreshold) {
            if (qty >= config.minTokens) {
                const lockedPnl = 1 - hedgeCostBasis;
                Logger.info(`   🛡️ HEDGE TRIGGER: CostBasis ${(hedgeCostBasis*100).toFixed(0)}¢ < Threshold ${(hedgeThreshold*100).toFixed(0)}¢`);

                const ok = await executeOrder(hedgeToken, side, qty, hedgeAsk, 'HEDGE', slug);
                if (ok) {
                    pos.hedged = true;
                    pos.hedgeQty = qty;
                    pos.hedgePrice = hedgeAsk;
                    pos.hedgeCost = cost;
                    if (lockedPnl >= 0) {
                        Logger.success(`   ✅ HEDGED ${side.toUpperCase()} @ ${(hedgeAsk*100).toFixed(1)}¢ | Locked +${(lockedPnl*100).toFixed(1)}¢/share profit`);
                    } else {
                        Logger.warning(`   🛡️ HEDGED ${side.toUpperCase()} @ ${(hedgeAsk*100).toFixed(1)}¢ | Capped loss at ${(lockedPnl*100).toFixed(1)}¢/share`);
                    }
                }
            } else {
                Logger.info(`   Position already balanced, no hedge needed`);
                pos.hedged = true;
            }
        } else if (minsLeft < 1) {
            // Last minute - only hedge if still under max threshold, otherwise let it ride
            if (hedgeCostBasis < config.hedgeMax && qty >= config.minTokens) {
                const lockedPnl = 1 - hedgeCostBasis;
                Logger.warning(`   ⚠️ FINAL HEDGE: <1min left, cost basis ${(hedgeCostBasis*100).toFixed(0)}¢ < max ${(config.hedgeMax*100).toFixed(0)}¢`);

                const ok = await executeOrder(hedgeToken, side, qty, hedgeAsk, 'HEDGE', slug);
                if (ok) {
                    pos.hedged = true;
                    pos.hedgeQty = qty;
                    pos.hedgePrice = hedgeAsk;
                    pos.hedgeCost = cost;
                    Logger.warning(`   🛡️ HEDGED ${side.toUpperCase()} @ ${(hedgeAsk*100).toFixed(1)}¢ | Loss capped at ${(lockedPnl*100).toFixed(1)}¢/share`);
                }
            } else {
                Logger.info(`   🎲 LET IT RIDE: Cost basis ${(hedgeCostBasis*100).toFixed(0)}¢ > max ${(config.hedgeMax*100).toFixed(0)}¢, not worth hedging`);
                pos.hedged = true;
            }
        } else {
            Logger.info(`   ⏳ Waiting: CostBasis ${(hedgeCostBasis*100).toFixed(0)}¢ >= Threshold ${(hedgeThreshold*100).toFixed(0)}¢`);
            // Don't mark as hedged yet - keep checking as threshold increases
        }
    }
}

async function checkResolutions(): Promise<void> {
    for (const [conditionId, pos] of positions.entries()) {
        if (pos.resolved) continue;
        if (Date.now() < pos.windowEnd + 60000) continue;

        const [yesBook, noBook] = await Promise.all([
            getOrderBook(pos.yesTokenId),
            getOrderBook(pos.noTokenId),
        ]);

        if (!yesBook && !noBook) {
            // Market resolved - fetch final prices
            try {
                const markets = await fetchMarketsBySlug(pos.marketSlug);
                if (markets.length > 0) {
                    const mAny = markets[0] as Record<string, unknown>;
                    const raw = mAny.outcomePrices;
                    let prices: number[] = [];
                    if (typeof raw === 'string') try { prices = JSON.parse(raw).map((p: string) => parseFloat(p)); } catch {}

                    if (prices.length >= 2) {
                        const winner: 'yes' | 'no' | null = prices[0] > 0.9 ? 'yes' : prices[1] > 0.9 ? 'no' : null;

                        if (winner) {
                            pos.resolved = true;
                            const totalCost = pos.initialCost + pos.oppositeCost + pos.hedgeCost;

                            const yesQty = (pos.initialSide === 'yes' ? pos.initialQty : 0) +
                                           (pos.oppositeSide === 'yes' ? pos.oppositeQty : 0) +
                                           (pos.hedged && pos.hedgeQty > 0 && calculateOptimalHedgeAmount(pos, 0.5).side === 'yes' ? pos.hedgeQty : 0);
                            const noQty = (pos.initialSide === 'no' ? pos.initialQty : 0) +
                                          (pos.oppositeSide === 'no' ? pos.oppositeQty : 0) +
                                          (pos.hedged && pos.hedgeQty > 0 && calculateOptimalHedgeAmount(pos, 0.5).side === 'no' ? pos.hedgeQty : 0);

                            const payout = winner === 'yes' ? yesQty : noQty;
                            pos.pnl = payout - totalCost;

                            if (config.dryRun) {
                                paperBalance += payout;
                                paperTotalPnl += pos.pnl;
                                if (pos.pnl >= 0) paperWins++; else paperLosses++;
                            }

                            Logger.header(`RESOLVED: ${pos.coin.toUpperCase()}`);
                            Logger.info(`Winner: ${winner.toUpperCase()}`);
                            Logger.info(`Cost: $${totalCost.toFixed(2)} | Payout: $${payout.toFixed(2)}`);
                            Logger.info(`P&L: ${pos.pnl >= 0 ? '+' : ''}$${pos.pnl.toFixed(2)}`);
                            if (config.dryRun) {
                                Logger.info(`[PAPER] Balance: $${paperBalance.toFixed(2)} | Total P&L: ${paperTotalPnl >= 0 ? '+' : ''}$${paperTotalPnl.toFixed(2)} | W/L: ${paperWins}/${paperLosses}`);
                            }
                            Logger.separator();
                        }
                    }
                }
            } catch {}
        }
    }
}

function logStats(): void {
    if (!config.dryRun) return;

    const active = Array.from(positions.values()).filter(p => !p.resolved).length;
    const resolved = Array.from(positions.values()).filter(p => p.resolved).length;
    const winRate = (paperWins + paperLosses) > 0 ? (paperWins / (paperWins + paperLosses) * 100).toFixed(1) : '0.0';

    Logger.info(`[PAPER STATS] Bal: $${paperBalance.toFixed(2)} | P&L: ${paperTotalPnl >= 0 ? '+' : ''}$${paperTotalPnl.toFixed(2)} | Win: ${winRate}% (${paperWins}W/${paperLosses}L) | Active: ${active} | Trades: ${paperTrades}`);
}

// ============================================================================
// MAIN
// ============================================================================
export const stopWindowStrategy = () => {
    isRunning = false;
    Logger.info('Window strategy shutdown requested...');
};

const windowStrategy = async (clobClient: ClobClient): Promise<void> => {
    _clobClient = clobClient;
    paperBalance = config.startingBalance;
    paperStartingBalance = config.startingBalance;

    Logger.header('WINDOW STRATEGY');
    if (config.dryRun) {
        Logger.warning('[PAPER MODE] No real trades - tracking paper P&L');
        Logger.info(`Starting Balance: $${paperBalance.toFixed(2)}`);
    } else {
        Logger.warning('[LIVE MODE] Real trades enabled!');
    }
    Logger.info(`Coins: ${config.coins.join(', ').toUpperCase()}`);
    Logger.info(`Entry: <${(config.entryThreshold*100).toFixed(0)}¢ | Opposite: <${(config.oppositeEntryThreshold*100).toFixed(0)}¢ (cost basis <${(config.combinedThresholdForOpposite*100).toFixed(0)}¢)`);
    Logger.info(`Hedge: Last ${config.hedgeMinutesBeforeEnd}min | Threshold: ${(config.hedgeStart*100).toFixed(0)}¢ → ${(config.hedgeMax*100).toFixed(0)}¢ (+${(config.hedgeRampPerMin*100).toFixed(0)}¢/min)`);
    Logger.info(`Initial size: $${config.initialBuyAmount.toFixed(2)}`);
    Logger.separator();

    let lastStats = 0;

    while (isRunning) {
        try {
            const windowTs = getCurrent15mWindowTimestamp();

            for (const coin of config.coins) {
                // Only check current window (not future)
                const ts = windowTs;
                const slug = generate15mSlug(coin, ts);
                const markets = await fetchMarketsBySlug(slug);

                for (const market of markets) {
                    const tokens = await getMarketTokens(market);
                    if (tokens) await processWindow(coin, market, tokens, ts);
                }
            }

            await checkResolutions();

            // Log stats every 5 min
            if (Date.now() - lastStats > 300000) {
                logStats();
                lastStats = Date.now();
            }

        } catch (err) {
            Logger.warning(`Window strategy error: ${err instanceof Error ? err.message : String(err)}`);
        }

        await new Promise(r => setTimeout(r, config.checkIntervalMs));
    }

    logStats();
    Logger.info('Window strategy stopped');
};

export default windowStrategy;
