/*-- 최근 20개 데이터 보기
SELECT 
    symbol,
    price,
    change_rate,
    volume,
    timestamp
FROM stock_prices
ORDER BY timestamp DESC
LIMIT 20;
*/
/*-- 최근 1시간 평균/최대/최소 근데 시간설정을 국내로 인해서 시차발생으로 10시간함.
SELECT 
    symbol,
    COUNT(*) as 수집_횟수,
    AVG(price) as 평균가,
    MAX(price) as 최고가,
    MIN(price) as 최저가,
    MAX(change_rate) as 최대변동률,
    AVG(volume) as 평균거래량
FROM stock_prices
WHERE timestamp > NOW() - INTERVAL '10 hour'
GROUP BY symbol;
*/

/*SELECT 
    NOW() as "DB의_현재시간", 
    MAX(timestamp) as "데이터에_찍힌_마지막시간",
    NOW() - MAX(timestamp) as "시간차이"
FROM stock_prices;*/

/*
-- 가격 변동폭 & 표준편차
SELECT 
    symbol,
    MAX(price) - MIN(price) as 변동폭,
    STDDEV(price) as 표준편차,
    (MAX(price) - MIN(price)) / AVG(price) * 100 as 변동률_퍼센트
FROM stock_prices
WHERE timestamp > NOW() - INTERVAL '1 day'
GROUP BY symbol;*/

-- 시간대별 평균가
SELECT 
    DATE_TRUNC('hour', timestamp) as 시간대,
    AVG(price) as 평균가,
    COUNT(*) as 데이터수
FROM stock_prices
WHERE timestamp > NOW() - INTERVAL '1 day'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY 시간대 DESC;