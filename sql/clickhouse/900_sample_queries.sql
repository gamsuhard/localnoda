-- Wallet history for one address in a time range
SELECT *
FROM tron_usdt_local.address_transfer_legs
WHERE address = {address:String}
  AND block_timestamp >= {time_gte:DateTime64(3, 'UTC')}
  AND block_timestamp < {time_lt:DateTime64(3, 'UTC')}
ORDER BY block_timestamp, tx_hash, log_index;

-- Counterparties for one address in a time range
SELECT
    counterparty_address,
    count() AS transfers,
    sumIf(amount_decimal, direction = 'outbound') AS sent_usdt,
    sumIf(amount_decimal, direction = 'inbound') AS received_usdt
FROM tron_usdt_local.address_transfer_legs
WHERE address = {address:String}
  AND block_timestamp >= {time_gte:DateTime64(3, 'UTC')}
  AND block_timestamp < {time_lt:DateTime64(3, 'UTC')}
GROUP BY counterparty_address
ORDER BY transfers DESC, counterparty_address ASC;

-- Tx lookup
SELECT *
FROM tron_usdt_local.trc20_transfer_events
WHERE tx_hash = {tx_hash:String}
ORDER BY log_index;
