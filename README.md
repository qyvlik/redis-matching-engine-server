# redis-matching-engine-server

Implement matching engine by lua script, run in redis

## export lua function

The [exchange.lua](./exchange.lua) sha256 value is `${EXCHANGE_LUA_HASH}`.

### `deposit_for_user`

deposit for user.

- `${USER_ID}`: user's id
- `${SYMBOL}`: symbol
- `${AMOUNT}`: deposit amount

return true or false.

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.deposit_for_user(ARGV[1], ARGV[2], ARGV[3] );" \
  0 ${USER_ID} ${SYMBOL} ${AMOUNT}
1) true
```

### `submit_order`

submit order to the order book, but do not match.

- `${USER_ID}`: user' id
- `${SYMBOL}`: base currency
- `${CURRENCY}`: quote currency
- `${SIDE}`: order side, such as buy or sell
- `${PRICE}`: order price
- `${AMOUNT}`: order amount
- `${TS}`: order create timestamp, unit is ms.

return the order id.

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.submit_order(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7] );" \
  0 ${USER_ID} ${SYMBOL} ${CURRENCY} ${SIDE} ${PRICE} ${AMOUNT} ${TS}
1) 10001
```

### `cancel_order`

remove the order from order book, and release the frozen asset for user.

- `${ORDER_ID}`: the order id which will be canceled.

return `1` if success.

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.cancel_order(ARGV[1]);" \
  0 ${ORDER_ID}
1) 1
```

### `match_order`

trigger match order.

- `${SYMBOL}`: base currency
- `${CURRENCY}`: quote currency
- `${LIMIT}`: hom mach size of the order will take from order book for match.
- `${TS}`: match timestamp, unit is ms.

return the size of order which be executed.

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.match_order(ARGV[1], ARGV[2], ARGV[3], ARGV[4] );" \
  0 ${SYMBOL} ${CURRENCY} ${LIMIT} ${TS}
1) 1
```

### `get_order_json`

get order json string.

- `${ORDER_ID}`: order id

return the order json string

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.get_order_json(ARGV[1]);" \
  0 ${ORDER_ID}
```

response such as follow:

```json
{
  "id": 1,
  "user_id": 1,
  "side": "buy",
  "stock_id": "BTC",
  "money_id": "USDT",
  "price": 4000.00,
  "amount": 1.0,
  "money": 4000.00,
  "process_amount": 0.0,
  "process_money": 0.0,
  "create_time": 1,
  "update_time": 1,
  "status": "create"
}
```

- `status`: order status, the value is `create`, `part`, `cancel`, `finish`

### `get_depth_json`

get the depth json string.

- `${SYMBOL}`: base currency
- `${CURRENCY}`: quote currency
- `${LIMIT}`: depth size

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.get_depth_json(ARGV[1], ARGV[2], ARGV[3] );" \
  0 ${SYMBOL} ${CURRENCY} ${LIMIT}
```

response such as follow:

```
{
  "asks": [
    [4000, 1],
    [4001, 1],
  ],
  "bids":[
    [3999, 1],
    [3998, 1],
  ]
}
```

### `get_last_price`

get the last match price.

- `${SYMBOL}`: base currency
- `${CURRENCY}`: quote currency

return the last price.

```bash
redis-cli> eval "local e = f_${EXCHANGE_LUA_HASH}(); \
  return e.get_last_price(ARGV[1], ARGV[2] );" \
  0 ${SYMBOL} ${CURRENCY}
1) 4000.00
```









