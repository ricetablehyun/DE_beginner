-- 실시간 데이터
SELECT * FROM realtime_stock_prices
ORDER BY received_at DESC
LIMIT 20;

-- 1분 집계
SELECT * FROM minute_aggregates
ORDER BY minute DESC;

-- 최근 10분 평균가
SELECT 
    minute,
    avg_price,
    count
FROM minute_aggregates
WHERE minute >= NOW() - INTERVAL '10 minutes'
ORDER BY minute DESC;

-- 실시간 변동성
SELECT 
    minute,
    max_price - min_price as volatility,
    (max_price - min_price) / avg_price * 100 as volatility_pct
FROM minute_aggregates
WHERE minute >= NOW() - INTERVAL '1 hour'
ORDER BY minute DESC;