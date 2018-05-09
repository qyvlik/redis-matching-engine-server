local function to_table_from_redis_hash(hash)
    local tmp = {};
    for k, v in pairs(hash) do
        if k % 2 == 0 then
            tmp[hash[k - 1]] = hash[k];
        end
    end
    return tmp;
end

local function table_is_empty(t)
    return _G.next(t) == nil;
end

local function get_json_from_redis_hash(key, field, value)
    local hash_in_redis = redis.call('HGETALL', key);

    if not hash_in_redis then
        return '{}';
    end

    local table = to_table_from_redis_hash(hash_in_redis);

    table[field] = value;

    return cjson.encode(table);
end

-- user asset
-- user_asset:available:1:btc
-- user_asset:frozen:1:usdt
local function add_user_available(user_id, coin, amount)
    redis.call('INCRBYFLOAT', 'user_asset:available:' .. user_id .. ':' .. coin, amount);
end

local function sub_user_available(user_id, coin, amount)
    redis.call('INCRBYFLOAT', 'user_asset:available:' .. user_id .. ':' .. coin, -amount);
end

local function add_user_frozen(user_id, coin, amount)
    redis.call('INCRBYFLOAT', 'user_asset:frozen:' .. user_id .. ':' .. coin, amount);
end

local function sub_user_frozen(user_id, coin, amount)
    redis.call('INCRBYFLOAT', 'user_asset:frozen:' .. user_id .. ':' .. coin, -amount);
end

local function get_user_available_asset(user_id, coin)
    local asset = redis.call('GET', 'user_asset:available:' .. user_id .. ':' .. coin);
    if not asset then
        redis.call('SET', 'user_asset:available:' .. user_id .. ':' .. coin, 0.0);
        return 0.0;
    end
    return tonumber(asset);
end

local function get_user_frozen_asset(user_id, coin)
    local asset = redis.call('GET', 'user_asset:frozen:' .. user_id .. ':' .. coin);
    if not asset then
        redis.call('SET', 'user_asset:frozen:' .. user_id .. ':' .. coin, 0.0);
        return 0.0;
    end
    return tonumber(asset);
end

local function deposit_for_user(user_id, coin, amount)
    add_user_available(user_id, coin, amount);
    return true;
end

local function withdraw_apply_user(user_id, coin, amount)
    sub_user_available(user_id, coin, amount);
    add_user_frozen(user_id, coin, amount);
end

local function withdraw_success_user(user_id, coin, amount)
    sub_user_frozen(user_id, coin, amount);
end

-- order
-- order:last
-- order:1
--  user_id
--  side
--  symbol
--  currency
--  price
--  amount
--  money
--  process_amount
--  process_money
--  create_time
--  update_time
--  status: create, part, cancel, finish

local function get_order_last_id()
    return redis.call('INCRBY', 'order:last', 1);
end

local function save_order(user_id, symbol, currency, side, price, amount, money, timestamp)
    local order_id = get_order_last_id();
    local pre_key = 'order:' .. order_id;

    redis.call('HSET', pre_key, 'user_id', user_id);
    redis.call('HSET', pre_key, 'side', side);
    redis.call('HSET', pre_key, 'symbol', symbol);
    redis.call('HSET', pre_key, 'currency', currency);
    redis.call('HSET', pre_key, 'price', price);
    redis.call('HSET', pre_key, 'amount', amount);
    redis.call('HSET', pre_key, 'money', money);
    redis.call('HSET', pre_key, 'create_time', timestamp);
    redis.call('HSET', pre_key, 'process_amount', 0.0);
    redis.call('HSET', pre_key, 'process_money', 0.0);
    redis.call('HSET', pre_key, 'status', 'create');
    return order_id;
end

local function get_order_field(order_id, field)
    local pre_key = 'order:' .. order_id;
    return redis.call('HGET', pre_key, field);
end

local function get_order_remain_amount(order_id)
    local amount = get_order_field(order_id, 'amount');
    local process_amount = get_order_field(order_id, 'process_amount');
    return tonumber(amount) - tonumber(process_amount);
end

local function get_order_remain_money(order_id)
    local money = get_order_field(order_id, 'money');
    local process_money = get_order_field(order_id, 'process_money');
    return tonumber(money) - tonumber(process_money);
end

local function add_order_process(order_id, amount, money)
    local pre_key = 'order:' .. order_id;
    local process_amount = get_order_field(order_id, 'process_amount');
    local process_money = get_order_field(order_id, 'process_money');
    process_amount = process_amount + amount;
    process_money = process_money + money;
    redis.call('HSET', pre_key, 'process_amount', process_amount);
    redis.call('HSET', pre_key, 'process_money', process_money);
end

local function modify_order_status(order_id, status)
    local pre_key = 'order:' .. order_id;
    redis.call('HSET', pre_key, 'status', status);
end

local function get_order_json(order_id)
    local key = 'order:' .. order_id;
    return get_json_from_redis_hash(key, 'id', order_id);
end

-- order_book:usdt:btc
local function update_depth_item(symbol, currency, side, price, amount)
    local order_book = 'order_book:' .. currency .. ':' .. symbol .. ':' .. side;
    local order_book_price = 'order_book_price:' .. currency .. ':' .. symbol .. ':' .. side;
    local amount_value = redis.call('HINCRBYFLOAT', order_book, price, amount);
    amount_value = tonumber(amount_value);

    if amount_value <= 0.0 then
        redis.call('HDEL', order_book, price);
        redis.call('SREM', order_book_price, price);
    else
        redis.call('SADD', order_book_price, price);
    end
end

local function get_single_depth(symbol, currency, side, limit)
    local order_book = 'order_book:' .. currency .. ':' .. symbol .. ':' .. side;
    local order_book_price = 'order_book_price:' .. currency .. ':' .. symbol .. ':' .. side;
    local sort = '';

    if side == 'buy' then
        sort = 'DESC';
    else
        sort = 'ASC';
    end

    local single_depth = {};

    local price_list = redis.call('SORT', order_book_price, sort, 'limit', 0, limit)

    for i = 1, #price_list do
        local price = price_list[i];
        local amount = redis.call('HGET', order_book, price);
        if not amount then
            -- do nothing
        else
            single_depth[i] = { price, amount };
        end
    end

    return single_depth;
end

local function get_depth_json(symbol, currency, limit)
    local ask_depth = get_single_depth(symbol, currency, 'sell', limit);
    local bid_depth = get_single_depth(symbol, currency, 'buy', limit);
    local depth = {
        symbol = symbol,
        currency = currency
    };

    if not table_is_empty(ask_depth) then
        depth['asks'] = ask_depth;
    end

    if not table_is_empty(bid_depth) then
        depth['bids'] = bid_depth;
    end

    return cjson.encode(depth);
end

-- order_book_list:usdt:btc:buy
-- score: price
-- member: order_id
local function put_into_order_book_list(symbol, currency, side, order_id, price)
    local key = 'order_book_list:' .. currency .. ':' .. symbol .. ':' .. side;
    redis.call('ZADD', key, price, order_id);           -- 升序排序
end

local function get_top_from_order_book_list(symbol, currency, side, limit)
    -- limit > 0
    local key = 'order_book_list:' .. currency .. ':' .. symbol .. ':' .. side;
    local from = 0;
    local to = 0;

    if side == 'buy' then
        from = -limit;
        to = -1;
    else
        from = 0;
        to = limit;
    end
    local order_book_range = redis.call('ZRANGE', key, from, to);
    return order_book_range;
end

local function remove_order_book(symbol, currency, side, order_id)
    local key = 'order_book_list:' .. currency .. ':' .. symbol .. ':' .. side;
    redis.call('ZREM', key, order_id);
end

-- match
-- match:usdt:btc:1
--  id
--  price
--  side
--  symbol
--  currency
--  price
--  amount
--  money
--  create_time

local function get_match_last_id()
    return redis.call('INCRBY', 'match:last', 1);
end

local function save_match(match)
    local match_id = get_match_last_id();
    match.id = match_id;

    local pre_key = 'match:' .. match_id;
    redis.call('HSET', pre_key, 'sell_order_id', match.sell_order_id);
    redis.call('HSET', pre_key, 'buy_order_id', match.buy_order_id);
    redis.call('HSET', pre_key, 'seller_id', match.seller_id);
    redis.call('HSET', pre_key, 'buyer_id', match.buyer_id);
    redis.call('HSET', pre_key, 'symbol', match.symbol);
    redis.call('HSET', pre_key, 'currency', match.currency);
    redis.call('HSET', pre_key, 'side', match.side);
    redis.call('HSET', pre_key, 'price', match.price);
    redis.call('HSET', pre_key, 'amount', match.amount);
    redis.call('HSET', pre_key, 'money', match.money);
    redis.call('HSET', pre_key, 'flag', match.flag);
    redis.call('HSET', pre_key, 'timestamp', match.timestamp);

    return match_id;
end

local function get_match_json(match_id)
    local key = 'match:' .. match_id;
    return get_json_from_redis_hash(key, 'id', match_id);
end

local function put_last_price(symbol, currency, price)
    local key = 'last_price:' .. currency .. ':' .. symbol;
    redis.call('SET', key, price);
end

local function get_last_price(symbol, currency)
    local key = 'last_price:' .. currency .. ':' .. symbol;
    local last_price = redis.call('GET', key);
    if not last_price then
        return 0.0;
    end
    return tonumber(last_price);
end

local function delivery_order(match)
    save_match(match);

    add_user_available(match.buyer_id, match.symbol, match.amount);
    sub_user_frozen(match.buyer_id, match.currency, match.money);
    add_order_process(match.buy_order_id, match.amount, match.money);

    local remain_money = get_order_remain_money(match.buy_order_id);
    if remain_money <= 0 then
        modify_order_status(match.buy_order_id, 'finish');
        remove_order_book(match.symbol, match.currency, 'buy', match.buy_order_id);
    else
        modify_order_status(match.buy_order_id, 'part');
    end

    local buy_order_price = get_order_field(match.buy_order_id, 'price');
    update_depth_item(match.symbol, match.currency, 'buy', buy_order_price, -match.amount);

    add_user_available(match.seller_id, match.currency, match.money);
    sub_user_frozen(match.seller_id, match.symbol, match.amount);
    add_order_process(match.sell_order_id, match.amount, match.money);

    local remain_amount = get_order_remain_amount(match.sell_order_id);
    if remain_amount <= 0 then
        modify_order_status(match.sell_order_id, 'finish');
        remove_order_book(match.symbol, match.currency, 'sell', match.sell_order_id);
    else
        modify_order_status(match.sell_order_id, 'part');
    end

    local sell_order_price = get_order_field(match.sell_order_id, 'price');
    update_depth_item(match.symbol, match.currency, 'sell', sell_order_price, -match.amount);

    put_last_price(match.price);
end

-- api

local function submit_order(user_id, symbol, currency, side, price, amount, timestamp)
    price = tonumber(price);
    amount = tonumber(amount);

    local money = price * amount;

    if price <= 0 then
        return -2;
    end

    if amount <= 0 then
        return -3;
    end

    local user_available = 0.0;
    local user_frozen = 0.0;
    local lock_asset_coin = '';
    local lock_asset_coin_amount = 0.0;

    if side == 'buy' then
        lock_asset_coin = currency;
        lock_asset_coin_amount = money;
    else
        lock_asset_coin = symbol;
        lock_asset_coin_amount = amount;
    end

    user_available = get_user_available_asset(user_id, lock_asset_coin);
    user_frozen = get_user_frozen_asset(user_id, lock_asset_coin);

    if lock_asset_coin_amount > user_available then
        return -1;
    end

    sub_user_available(user_id, lock_asset_coin, lock_asset_coin_amount);

    add_user_frozen(user_id, lock_asset_coin, lock_asset_coin_amount);

    local order_id = save_order(user_id, symbol, currency, side, price, amount, money, timestamp);

    put_into_order_book_list(symbol, currency, side, order_id, price);

    update_depth_item(symbol, currency, side, price, amount);

    return order_id;
end

local function cancel_order(order_id)
    local order_status = get_order_field(order_id, 'status');
    if order_status == 'cancel' or order_status == 'finish' then
        return -1;
    end

    local side = get_order_field(order_id, 'side');
    local unlock_asset_coin = '';
    local unlock_asset_coin_amount = 0.0;
    local user_id = get_order_field(order_id, 'user_id');
    local price = get_order_field(order_id, 'price');
    local symbol = get_order_field(order_id, 'symbol');
    local currency = get_order_field(order_id, 'currency');
    local money = get_order_field(order_id, 'money');
    local process_money = get_order_field(order_id, 'process_money');
    local amount = get_order_field(order_id, 'amount');
    local process_amount = get_order_field(order_id, 'process_amount');

    if side == 'buy' then
        unlock_asset_coin = currency;
        unlock_asset_coin_amount = tonumber(money) - tonumber(process_money);
    else
        unlock_asset_coin = symbol;
        unlock_asset_coin_amount = tonumber(amount) - tonumber(process_amount);
    end

    modify_order_status(order_id, 'cancel');
    remove_order_book(symbol, currency, side, order_id);

    add_user_available(user_id, unlock_asset_coin, unlock_asset_coin_amount);
    sub_user_frozen(user_id, unlock_asset_coin, unlock_asset_coin_amount);

    update_depth_item(symbol, currency, side, price, -unlock_asset_coin_amount);

    return 1;
end

local SELL_ORDER_FILL = 0;
local BUY_ORDER_FILL = 1;
local BUY_ORDER_AND_ORDER_SELL_FILL = 2;

local function match_order_once(symbol, currency, ask1_id, bid1_id, timestamp)

    if not ask1_id or not bid1_id then
        return false;
    end

    local ask1_price = tonumber(get_order_field(ask1_id, 'price'));
    local bid1_price = tonumber(get_order_field(bid1_id, 'price'));

    if ask1_price > bid1_price then
        return false;
    end

    local seller_id = get_order_field(ask1_id, 'user_id');
    local buyer_id = get_order_field(bid1_id, 'user_id');

    local match = {};

    match.sell_order_id = ask1_id;
    match.buy_order_id = bid1_id;
    match.seller_id = seller_id;
    match.buyer_id = buyer_id;
    match.symbol = symbol;
    match.currency = currency;
    match.timestamp = timestamp;

    if ask1_id < bid1_id then
        match.side = 'buy';
        match.price = ask1_price;
    else
        match.side = 'sell';
        match.price = bid1_price;
    end

    local ask_amount = get_order_remain_amount(ask1_id);
    local bid_money = get_order_remain_money(bid1_id);
    local bid_get_amount = bid_money / match.price;

    if ask_amount == bid_get_amount then
        match.amount = ask_amount * 1;
        match.money = ask_amount * match.price;
        match.flag = BUY_ORDER_AND_ORDER_SELL_FILL;
    else
        if ask_amount > bid_get_amount then
            match.amount = bid_get_amount * 1;
            match.money = bid_get_amount * match.price;
            match.flag = BUY_ORDER_FILL;
        else
            match.amount = ask_amount * 1;
            match.money = ask_amount * match.price;
            match.flag = SELL_ORDER_FILL;
        end
    end

    delivery_order(match);

    return true;
end

local function match_order(symbol, currency, limit, timestamp)
    local askList = get_top_from_order_book_list(symbol, currency, 'sell', limit);
    local bidList = get_top_from_order_book_list(symbol, currency, 'buy', limit);

    local match_count = 0;
    local running = true;
    repeat
        if #askList == 0 or #bidList == 0 then
            break ;
        end

        local ask1_id = table.remove(askList, 1);
        local bid1_id = table.remove(bidList, 1);
        running = match_order_once(symbol, currency, ask1_id, bid1_id, timestamp);
        if running then
            match_count = match_count + 1;
        end
    until not running;

    return match_count;
end

local exchange = {
    get_user_available_asset = get_user_available_asset,
    get_user_frozen_asset = get_user_frozen_asset,
    deposit_for_user = deposit_for_user,
    withdraw_apply_user = withdraw_apply_user,
    withdraw_success_user = withdraw_success_user,
    submit_order = submit_order,
    cancel_order = cancel_order,
    match_order = match_order,
    get_match_json = get_match_json,
    get_order_json = get_order_json,
    get_depth_json = get_depth_json,
    get_last_price = get_last_price
}

return exchange;
