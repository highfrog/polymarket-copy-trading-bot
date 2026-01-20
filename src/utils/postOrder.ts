import { ClobClient, OrderType, Side } from '@polymarket/clob-client';
import { ENV } from '../config/env';
import { UserActivityInterface, UserPositionInterface } from '../interfaces/User';
import { getUserActivityModel } from '../models/userHistory';
import Logger from './logger';
import { calculateOrderSize, getTradeMultiplier } from '../config/copyStrategy';

const RETRY_LIMIT = ENV.RETRY_LIMIT;
const COPY_STRATEGY_CONFIG = ENV.COPY_STRATEGY_CONFIG;
const SKIP_SLIPPAGE_CHECK = ENV.SKIP_SLIPPAGE_CHECK;

// Legacy parameters (for backward compatibility in SELL logic)
const TRADE_MULTIPLIER = ENV.TRADE_MULTIPLIER;
const COPY_PERCENTAGE = ENV.COPY_PERCENTAGE;

// Polymarket minimum order sizes for GTC orders
const MIN_ORDER_SIZE_USD = 0.10; // Minimum order size in USD for GTC BUY orders
const MIN_ORDER_SIZE_TOKENS = 0.10; // Minimum order size in tokens for GTC SELL/MERGE orders

// Polymarket decimal precision limits - use string conversion to avoid floating point issues
const roundToDecimals = (value: number, decimals: number): number => {
    const factor = Math.pow(10, decimals);
    const rounded = Math.floor(value * factor) / factor;
    // Convert to string and back to eliminate floating point artifacts
    return parseFloat(rounded.toFixed(decimals));
};
const roundAmount = (amount: number): number => roundToDecimals(amount, 2); // Taker/USD amounts: 2 decimals
const roundTokens = (tokens: number): number => roundToDecimals(tokens, 4); // Maker/token amounts: 4 decimals
const roundPrice = (price: number): number => roundToDecimals(price, 2); // Prices: 2 decimals

const extractOrderError = (response: unknown): string | undefined => {
    if (!response) {
        return undefined;
    }

    if (typeof response === 'string') {
        return response;
    }

    if (typeof response === 'object') {
        const data = response as Record<string, unknown>;

        const directError = data.error;
        if (typeof directError === 'string') {
            return directError;
        }

        if (typeof directError === 'object' && directError !== null) {
            const nested = directError as Record<string, unknown>;
            if (typeof nested.error === 'string') {
                return nested.error;
            }
            if (typeof nested.message === 'string') {
                return nested.message;
            }
        }

        if (typeof data.errorMsg === 'string') {
            return data.errorMsg;
        }

        if (typeof data.message === 'string') {
            return data.message;
        }
    }

    return undefined;
};

const isInsufficientBalanceOrAllowanceError = (message: string | undefined): boolean => {
    if (!message) {
        return false;
    }
    const lower = message.toLowerCase();
    return lower.includes('not enough balance') || lower.includes('allowance');
};

const postOrder = async (
    clobClient: ClobClient,
    condition: string,
    my_position: UserPositionInterface | undefined,
    user_position: UserPositionInterface | undefined,
    trade: UserActivityInterface,
    my_balance: number,
    user_balance: number,
    userAddress: string,
    preScaledAmount?: number // Optional: skip scaling if provided (for aggregated trades)
) => {
    const UserActivity = getUserActivityModel(userAddress);
    //Merge strategy
    if (condition === 'merge') {
        Logger.info('Executing MERGE strategy...');
        if (!my_position) {
            Logger.warning('No position to merge');
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            return;
        }
        let remaining = roundTokens(my_position.size); // Round to 5 decimals

        // Check minimum order size
        if (remaining < MIN_ORDER_SIZE_TOKENS) {
            Logger.warning(
                `Position size (${remaining.toFixed(4)} tokens) too small to merge - skipping`
            );
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            return;
        }

        let retry = 0;
        let abortDueToFunds = false;
        while (remaining > 0 && retry < RETRY_LIMIT) {
            const orderBook = await clobClient.getOrderBook(trade.asset);
            if (!orderBook.bids || orderBook.bids.length === 0) {
                Logger.warning('No bids available in order book');
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                break;
            }

            const maxPriceBid = orderBook.bids.reduce((max, bid) => {
                return parseFloat(bid.price) > parseFloat(max.price) ? bid : max;
            }, orderBook.bids[0]);

            Logger.info(`Best bid: ${maxPriceBid.size} @ $${maxPriceBid.price}`);
            let order_arges;
            if (remaining <= parseFloat(maxPriceBid.size)) {
                order_arges = {
                    side: Side.SELL,
                    tokenID: my_position.asset,
                    amount: roundTokens(remaining), // Round to 5 decimals
                    price: roundPrice(parseFloat(maxPriceBid.price)),
                };
            } else {
                order_arges = {
                    side: Side.SELL,
                    tokenID: my_position.asset,
                    amount: roundTokens(parseFloat(maxPriceBid.size)), // Round to 5 decimals
                    price: roundPrice(parseFloat(maxPriceBid.price)),
                };
            }
            // Order args logged internally
            const signedOrder = await clobClient.createMarketOrder(order_arges);
            const resp = await clobClient.postOrder(signedOrder, OrderType.GTC);
            if (resp.success === true) {
                retry = 0;
                Logger.orderResult(
                    true,
                    `Sold ${order_arges.amount} tokens at $${order_arges.price}`
                );
                remaining -= order_arges.amount;
            } else {
                const errorMessage = extractOrderError(resp);
                if (isInsufficientBalanceOrAllowanceError(errorMessage)) {
                    abortDueToFunds = true;
                    Logger.warning(
                        `Order rejected: ${errorMessage || 'Insufficient balance or allowance'}`
                    );
                    Logger.warning(
                        'Skipping remaining attempts. Top up funds or run `npm run check-allowance` before retrying.'
                    );
                    break;
                }
                retry += 1;
                Logger.warning(
                    `Order failed (attempt ${retry}/${RETRY_LIMIT})${errorMessage ? ` - ${errorMessage}` : ''}`
                );
            }
        }
        if (abortDueToFunds) {
            await UserActivity.updateOne(
                { _id: trade._id },
                { bot: true, botExcutedTime: RETRY_LIMIT }
            );
            return;
        }
        if (retry >= RETRY_LIMIT) {
            await UserActivity.updateOne({ _id: trade._id }, { bot: true, botExcutedTime: retry });
        } else {
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
        }
    } else if (condition === 'buy') {
        //Buy strategy
        Logger.info(`Executing BUY strategy... (slippage check: ${SKIP_SLIPPAGE_CHECK ? 'DISABLED' : 'enabled'})`);

        Logger.info(`Your balance: $${my_balance.toFixed(2)}`);
        Logger.info(`Trader bought: $${trade.usdcSize.toFixed(2)}`);

        let remaining: number;

        // If preScaledAmount provided (aggregated trade), use it directly
        if (preScaledAmount !== undefined) {
            remaining = roundAmount(preScaledAmount); // Round to 2 decimals
            Logger.info(`📊 Using pre-scaled amount: $${remaining.toFixed(2)} (aggregated trade)`);
        } else {
            // Get current position size for position limit checks
            const currentPositionValue = my_position ? my_position.size * my_position.avgPrice : 0;

            // Use new copy strategy system
            const orderCalc = calculateOrderSize(
                COPY_STRATEGY_CONFIG,
                trade.usdcSize,
                my_balance,
                currentPositionValue
            );

            // Log the calculation reasoning
            Logger.info(`📊 ${orderCalc.reasoning}`);

            // Check if order should be executed
            if (orderCalc.belowMinimum) {
                Logger.warning(`❌ Cannot execute: ${orderCalc.reasoning}`);
                Logger.warning(`💡 Increase COPY_SIZE or wait for larger trades`);
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                return;
            }

            remaining = roundAmount(orderCalc.finalAmount); // Round to 2 decimals
        }

        let retry = 0;
        let abortDueToFunds = false;
        let totalBoughtTokens = 0; // Track total tokens bought for this trade

        while (remaining > 0 && retry < RETRY_LIMIT) {
            const orderBook = await clobClient.getOrderBook(trade.asset);
            if (!orderBook.asks || orderBook.asks.length === 0) {
                Logger.warning('No asks available in order book');
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                break;
            }

            const minPriceAsk = orderBook.asks.reduce((min, ask) => {
                return parseFloat(ask.price) < parseFloat(min.price) ? ask : min;
            }, orderBook.asks[0]);

            Logger.info(`Best ask: ${minPriceAsk.size} @ $${minPriceAsk.price}`);

            // Slippage check: skip if best ask is too far above trader's price
            // Can be disabled via SKIP_SLIPPAGE_CHECK=true for GTC maker orders
            if (!SKIP_SLIPPAGE_CHECK) {
                const maxAcceptablePrice = trade.price * 1.10;  // 10% above trader's price
                if (parseFloat(minPriceAsk.price) > maxAcceptablePrice) {
                    Logger.warning(`Price slippage too high: best ask $${minPriceAsk.price} > max $${maxAcceptablePrice.toFixed(2)} - skipping`);
                    await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                    break;
                }
            }

            // Check if remaining amount is below minimum before creating order
            if (remaining < MIN_ORDER_SIZE_USD) {
                Logger.info(
                    `Remaining amount ($${remaining.toFixed(2)}) below minimum - completing trade`
                );
                await UserActivity.updateOne(
                    { _id: trade._id },
                    { bot: true, myBoughtSize: totalBoughtTokens }
                );
                break;
            }

            // Use trader's execution price to ensure order is a maker (sits on book)
            // Taker orders need 5 tokens min, Maker orders need $1.00 USD min
            const traderPrice = roundPrice(trade.price);
            const bestAskPrice = roundPrice(parseFloat(minPriceAsk.price));

            // Polymarket minimum sizes:
            // - Taker orders (price >= best ask): minimum 5 tokens
            // - Maker orders (price < best ask): minimum $1.00 USD
            const TAKER_MIN_TOKENS = 5;
            const MAKER_MIN_USD = 1.00;

            // Calculate approximate tokens and USD
            const approxTokens = remaining / traderPrice;
            const approxUsd = remaining;

            // Order strategy for arb trading:
            // - FOK for >= $1 USD: Fast execution at best ask (marketable, needs $1 min)
            // - GTC for >= 5 tokens but < $1 USD: Price BELOW best ask to sit on book (maker, needs 5 token min)
            // - Below both: shouldn't happen with threshold aggregation
            let price: number;
            let useFOK = false;

            if (approxUsd >= MAKER_MIN_USD) {
                // >= $1 USD: Use FOK for fast arb execution at best available price
                useFOK = true;
                price = bestAskPrice;  // Best available price for immediate fill
                Logger.info(`⚡ FOK order: $${approxUsd.toFixed(2)} >= $${MAKER_MIN_USD} | fast execution @ best ask $${bestAskPrice.toFixed(2)}`);
            } else if (approxTokens >= TAKER_MIN_TOKENS) {
                // >= 5 tokens but < $1 USD: Price BELOW best ask to be a maker (not marketable)
                // Use 2% buffer or $0.01, whichever is larger, to ensure we're below best ask
                const buffer = Math.max(0.01, bestAskPrice * 0.02);
                const makerPrice = roundPrice(bestAskPrice - buffer);
                // Use lower of trader's price or our calculated maker price
                price = Math.min(traderPrice, makerPrice);
                // Ensure we're definitely below best ask
                if (price >= bestAskPrice) {
                    price = roundPrice(bestAskPrice - 0.01);
                }
                Logger.info(`📋 GTC maker: ${approxTokens.toFixed(2)} tokens >= ${TAKER_MIN_TOKENS}, $${approxUsd.toFixed(2)} < $${MAKER_MIN_USD} | price $${price.toFixed(2)} < best ask $${bestAskPrice.toFixed(2)}`);
            } else {
                // Below both minimums - should not happen with threshold-based aggregation
                Logger.warning(`Order too small: ${approxTokens.toFixed(2)} tokens < ${TAKER_MIN_TOKENS} AND $${approxUsd.toFixed(2)} < $${MAKER_MIN_USD}`);
                Logger.warning(`This should not happen - check aggregation thresholds`);
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                break;
            }

            // PRECISION RULES (opposite for market vs limit orders!):
            // - FOK (market): USD max 2 decimals, tokens max 4 decimals
            // - GTC (limit):  USD max 4 decimals, tokens max 2 decimals

            // Step 1: Price as integer cents → clean 2-decimal float
            const priceCents = Math.round(price * 100);
            const priceStr = (priceCents / 100).toFixed(2);
            const finalPrice = parseFloat(priceStr);

            let finalTokens: number;
            let finalUsdCost: number;
            let tokensStr: string;
            let usdStr: string;

            if (useFOK) {
                // FOK: USD must be 2 decimals, tokens can be 4 decimals
                const usdCents = Math.floor(remaining * 100);
                usdStr = (usdCents / 100).toFixed(2);
                finalUsdCost = parseFloat(usdStr);

                // Tokens = USD / price (can have 4 decimals)
                const tokensRaw = finalUsdCost / finalPrice;
                const tokensTenThousandths = Math.floor(tokensRaw * 10000);
                tokensStr = (tokensTenThousandths / 10000).toFixed(4);
                finalTokens = parseFloat(tokensStr);
            } else {
                // GTC: Tokens must be 2 decimals, USD can be 4 decimals
                const tokensCents = Math.floor(remaining * 10000 / priceCents);
                tokensStr = (tokensCents / 100).toFixed(2);
                finalTokens = parseFloat(tokensStr);

                // USD = tokens * price (can have 4 decimals)
                const usdTenThousandths = tokensCents * priceCents;
                usdStr = (usdTenThousandths / 10000).toFixed(4);
                finalUsdCost = parseFloat(usdStr);
            }

            Logger.info(`Order: $${usdStr} for ${tokensStr} tokens @ $${priceStr}`);

            // Skip if rounding resulted in zero tokens
            if (finalTokens === 0) {
                Logger.info(`Order too small after rounding - completing trade`);
                await UserActivity.updateOne(
                    { _id: trade._id },
                    { bot: true, myBoughtSize: totalBoughtTokens }
                );
                break;
            }

            let signedOrder;
            let resp;

            try {
                if (useFOK) {
                    // FOK: Use createMarketOrder with amount (USD, 2 decimals)
                    const order_args = {
                        side: Side.BUY,
                        tokenID: trade.asset,
                        amount: finalUsdCost,  // USD rounded to 2 decimals
                        price: finalPrice,
                    };
                    Logger.info(`Creating FOK market order: amount=$${finalUsdCost}, price=$${finalPrice}`);
                    signedOrder = await clobClient.createMarketOrder(order_args);
                } else {
                    // GTC: Use createOrder with size (tokens, 2 decimals)
                    const order_args = {
                        side: Side.BUY,
                        tokenID: trade.asset,
                        size: finalTokens,   // Tokens rounded to 2 decimals
                        price: finalPrice,
                    };
                    Logger.info(`Creating GTC limit order: size=${finalTokens} tokens, price=$${finalPrice}`);
                    signedOrder = await clobClient.createOrder(order_args, { tickSize: "0.01" as const });
                }

                const orderType = useFOK ? OrderType.FOK : OrderType.GTC;
                resp = await clobClient.postOrder(signedOrder, orderType);
            } catch (err) {
                // Handle CLOB client errors (network issues, etc.)
                const errMsg = err instanceof Error ? err.message : String(err);
                Logger.warning(`CLOB client error: ${errMsg}`);

                // Check for network/connection errors
                if (errMsg.toLowerCase().includes('network') ||
                    errMsg.toLowerCase().includes('timeout') ||
                    errMsg.toLowerCase().includes('econnreset') ||
                    errMsg.toLowerCase().includes('socket') ||
                    errMsg.toLowerCase().includes('fetch')) {
                    Logger.warning(`Network error - waiting 3s before retry...`);
                    await new Promise(resolve => setTimeout(resolve, 3000));
                    retry += 1;
                    continue;
                }

                // For other errors, increment retry and continue
                retry += 1;
                if (retry < RETRY_LIMIT) {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
                continue;
            }
            if (resp.success === true) {
                retry = 0;
                totalBoughtTokens += finalTokens;
                Logger.orderResult(
                    true,
                    `Bought ${tokensStr} tokens at $${priceStr} ($${usdStr})`
                );
                remaining -= finalUsdCost;
            } else {
                const errorMessage = extractOrderError(resp);
                if (isInsufficientBalanceOrAllowanceError(errorMessage)) {
                    abortDueToFunds = true;
                    Logger.warning(
                        `Order rejected: ${errorMessage || 'Insufficient balance or allowance'}`
                    );
                    Logger.warning(
                        'Skipping remaining attempts. Top up funds or run `npm run check-allowance` before retrying.'
                    );
                    break;
                }

                // Check if it's a size minimum error (< 5 tokens for taker)
                const isSizeMinError = errorMessage?.includes('lower than the minimum: 5');
                if (isSizeMinError) {
                    Logger.warning(`Order size ${finalTokens} tokens below 5 token minimum.`);
                    Logger.warning(`This shouldn't happen - check aggregation thresholds.`);
                    await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                    break;
                }

                // Check for decimal precision errors (shouldn't happen with our rounding)
                const isDecimalError = errorMessage?.includes('decimal') || errorMessage?.includes('accuracy');
                if (isDecimalError) {
                    Logger.warning(`Decimal precision error - tokens: ${finalTokens}, price: ${finalPrice}`);
                    await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                    break;
                }

                // Check for rate limit / network errors - back off before retrying
                const isRateLimitError = errorMessage?.toLowerCase().includes('rate') ||
                                         errorMessage?.toLowerCase().includes('limit') ||
                                         errorMessage?.toLowerCase().includes('too many');
                const isNetworkError = errorMessage?.toLowerCase().includes('network') ||
                                       errorMessage?.toLowerCase().includes('timeout') ||
                                       errorMessage?.toLowerCase().includes('econnreset') ||
                                       errorMessage?.toLowerCase().includes('socket');

                if (isRateLimitError || isNetworkError) {
                    const delay = isRateLimitError ? 5000 : 2000;  // 5s for rate limit, 2s for network
                    Logger.warning(`${isRateLimitError ? 'Rate limited' : 'Network error'} - waiting ${delay/1000}s before retry...`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                }

                retry += 1;
                Logger.warning(
                    `Order failed (attempt ${retry}/${RETRY_LIMIT})${errorMessage ? ` - ${errorMessage}` : ''}`
                );

                // Small delay between retries to avoid hammering the API
                if (retry < RETRY_LIMIT) {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        }
        if (abortDueToFunds) {
            await UserActivity.updateOne(
                { _id: trade._id },
                { bot: true, botExcutedTime: RETRY_LIMIT, myBoughtSize: totalBoughtTokens }
            );
            return;
        }
        if (retry >= RETRY_LIMIT) {
            await UserActivity.updateOne(
                { _id: trade._id },
                { bot: true, botExcutedTime: retry, myBoughtSize: totalBoughtTokens }
            );
        } else {
            await UserActivity.updateOne(
                { _id: trade._id },
                { bot: true, myBoughtSize: totalBoughtTokens }
            );
        }

        // Log the tracked purchase for later sell reference
        if (totalBoughtTokens > 0) {
            Logger.info(
                `📝 Tracked purchase: ${totalBoughtTokens.toFixed(2)} tokens for future sell calculations`
            );
        }
    } else if (condition === 'sell') {
        //Sell strategy
        Logger.info('Executing SELL strategy...');
        let remaining = 0;
        if (!my_position) {
            Logger.warning('No position to sell');
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            return;
        }

        // Get all previous BUY trades for this asset to calculate total bought
        const previousBuys = await UserActivity.find({
            asset: trade.asset,
            conditionId: trade.conditionId,
            side: 'BUY',
            bot: true,
            myBoughtSize: { $exists: true, $gt: 0 },
        }).exec();

        const totalBoughtTokens = previousBuys.reduce(
            (sum, buy) => sum + (buy.myBoughtSize || 0),
            0
        );

        if (totalBoughtTokens > 0) {
            Logger.info(
                `📊 Found ${previousBuys.length} previous purchases: ${totalBoughtTokens.toFixed(2)} tokens bought`
            );
        }

        if (!user_position) {
            // Trader sold entire position - we sell entire position too
            remaining = roundTokens(my_position.size); // Round to 5 decimals
            Logger.info(
                `Trader closed entire position → Selling all your ${remaining.toFixed(4)} tokens`
            );
        } else {
            // Calculate the % of position the trader is selling
            const trader_sell_percent = trade.size / (user_position.size + trade.size);
            const trader_position_before = user_position.size + trade.size;

            Logger.info(
                `Position comparison: Trader has ${trader_position_before.toFixed(2)} tokens, You have ${my_position.size.toFixed(2)} tokens`
            );
            Logger.info(
                `Trader selling: ${trade.size.toFixed(2)} tokens (${(trader_sell_percent * 100).toFixed(2)}% of their position)`
            );

            // Use tracked bought tokens if available, otherwise fallback to current position
            let baseSellSize;
            if (totalBoughtTokens > 0) {
                baseSellSize = totalBoughtTokens * trader_sell_percent;
                Logger.info(
                    `Calculating from tracked purchases: ${totalBoughtTokens.toFixed(2)} × ${(trader_sell_percent * 100).toFixed(2)}% = ${baseSellSize.toFixed(2)} tokens`
                );
            } else {
                baseSellSize = my_position.size * trader_sell_percent;
                Logger.warning(
                    `No tracked purchases found, using current position: ${my_position.size.toFixed(2)} × ${(trader_sell_percent * 100).toFixed(2)}% = ${baseSellSize.toFixed(2)} tokens`
                );
            }

            // Apply tiered or single multiplier based on trader's order size (symmetrical with BUY logic)
            const multiplier = getTradeMultiplier(COPY_STRATEGY_CONFIG, trade.usdcSize);
            remaining = roundTokens(baseSellSize * multiplier); // Round to 5 decimals

            if (multiplier !== 1.0) {
                Logger.info(
                    `Applying ${multiplier}x multiplier (based on trader's $${trade.usdcSize.toFixed(2)} order): ${baseSellSize.toFixed(2)} → ${remaining.toFixed(4)} tokens`
                );
            }
        }

        // Check minimum order size
        if (remaining < MIN_ORDER_SIZE_TOKENS) {
            Logger.warning(
                `❌ Cannot execute: Sell amount ${remaining.toFixed(4)} tokens below minimum (${MIN_ORDER_SIZE_TOKENS} token)`
            );
            Logger.warning(`💡 This happens when position sizes are too small or mismatched`);
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
            return;
        }

        // Cap sell amount to available position size
        if (remaining > my_position.size) {
            Logger.warning(
                `⚠️  Calculated sell ${remaining.toFixed(4)} tokens > Your position ${my_position.size.toFixed(4)} tokens`
            );
            Logger.warning(`Capping to maximum available: ${my_position.size.toFixed(4)} tokens`);
            remaining = roundTokens(my_position.size); // Round to 5 decimals
        }

        let retry = 0;
        let abortDueToFunds = false;
        let totalSoldTokens = 0; // Track total tokens sold

        while (remaining > 0 && retry < RETRY_LIMIT) {
            const orderBook = await clobClient.getOrderBook(trade.asset);
            if (!orderBook.bids || orderBook.bids.length === 0) {
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                Logger.warning('No bids available in order book');
                break;
            }

            const maxPriceBid = orderBook.bids.reduce((max, bid) => {
                return parseFloat(bid.price) > parseFloat(max.price) ? bid : max;
            }, orderBook.bids[0]);

            Logger.info(`Best bid: ${maxPriceBid.size} @ $${maxPriceBid.price}`);

            // Check if remaining amount is below minimum before creating order
            if (remaining < MIN_ORDER_SIZE_TOKENS) {
                Logger.info(
                    `Remaining amount (${remaining.toFixed(2)} tokens) below minimum - completing trade`
                );
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                break;
            }

            const sellAmount = roundTokens(Math.min(remaining, parseFloat(maxPriceBid.size))); // Round to 5 decimals

            // Final check: don't create orders below minimum
            if (sellAmount < MIN_ORDER_SIZE_TOKENS) {
                Logger.info(
                    `Order amount (${sellAmount.toFixed(4)} tokens) below minimum - completing trade`
                );
                await UserActivity.updateOne({ _id: trade._id }, { bot: true });
                break;
            }

            const order_arges = {
                side: Side.SELL,
                tokenID: trade.asset,
                amount: sellAmount,
                price: roundPrice(parseFloat(maxPriceBid.price)),
            };
            // Order args logged internally
            const signedOrder = await clobClient.createMarketOrder(order_arges);
            const resp = await clobClient.postOrder(signedOrder, OrderType.GTC);
            if (resp.success === true) {
                retry = 0;
                totalSoldTokens += order_arges.amount;
                Logger.orderResult(
                    true,
                    `Sold ${order_arges.amount} tokens at $${order_arges.price}`
                );
                remaining -= order_arges.amount;
            } else {
                const errorMessage = extractOrderError(resp);
                if (isInsufficientBalanceOrAllowanceError(errorMessage)) {
                    abortDueToFunds = true;
                    Logger.warning(
                        `Order rejected: ${errorMessage || 'Insufficient balance or allowance'}`
                    );
                    Logger.warning(
                        'Skipping remaining attempts. Top up funds or run `npm run check-allowance` before retrying.'
                    );
                    break;
                }
                retry += 1;
                Logger.warning(
                    `Order failed (attempt ${retry}/${RETRY_LIMIT})${errorMessage ? ` - ${errorMessage}` : ''}`
                );
            }
        }

        // Update tracked purchases after successful sell
        if (totalSoldTokens > 0 && totalBoughtTokens > 0) {
            const sellPercentage = totalSoldTokens / totalBoughtTokens;

            if (sellPercentage >= 0.99) {
                // Sold essentially all tracked tokens - clear tracking
                await UserActivity.updateMany(
                    {
                        asset: trade.asset,
                        conditionId: trade.conditionId,
                        side: 'BUY',
                        bot: true,
                        myBoughtSize: { $exists: true, $gt: 0 },
                    },
                    { $set: { myBoughtSize: 0 } }
                );
                Logger.info(
                    `🧹 Cleared purchase tracking (sold ${(sellPercentage * 100).toFixed(1)}% of position)`
                );
            } else {
                // Partial sell - reduce tracked purchases proportionally
                for (const buy of previousBuys) {
                    const newSize = (buy.myBoughtSize || 0) * (1 - sellPercentage);
                    await UserActivity.updateOne(
                        { _id: buy._id },
                        { $set: { myBoughtSize: newSize } }
                    );
                }
                Logger.info(
                    `📝 Updated purchase tracking (sold ${(sellPercentage * 100).toFixed(1)}% of tracked position)`
                );
            }
        }

        if (abortDueToFunds) {
            await UserActivity.updateOne(
                { _id: trade._id },
                { bot: true, botExcutedTime: RETRY_LIMIT }
            );
            return;
        }
        if (retry >= RETRY_LIMIT) {
            await UserActivity.updateOne({ _id: trade._id }, { bot: true, botExcutedTime: retry });
        } else {
            await UserActivity.updateOne({ _id: trade._id }, { bot: true });
        }
    } else {
        Logger.error(`Unknown condition: ${condition}`);
    }
};

export default postOrder;
