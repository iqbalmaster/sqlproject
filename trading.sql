select  top 2 * from trading_practice_dataset
----Retrieve all trades for ticker AAPL in February 2023.
select * from trading_practice_dataset
where Ticker = 'AAPL' 
AND Date BETWEEN  '2023-02-01' AND '2023-02-28';
---Find the total trading volume for each ticker.
select Ticker, sum(Volume) as total_volume from trading_practice_dataset
group by Ticker
order by total_volume DESC
---Calculate the average closing price of each ticker.
select Ticker,
AVG([Close]) as avg_closing_price from trading_practice_dataset
group by Ticker
---Which ticker had the largest average daily profit opportunity (Close − Open)?


order by avg_closing_price Desc
---Which ticker had the highest average daily volume?
SELECT top 1 Ticker, AVG(Volume) as avg_volume from trading_practice_dataset
group by Ticker
order by avg_volume DESC
---Get the top 10 highest volume trades across all tickers.
select top 10 * from trading_practice_dataset
order by Volume desc

---Find the day with the highest single-day gain (Close − Open).
select top 1 *, ([Close] - [Open]) as single_day_gain from trading_practice_dataset
order by single_day_gain DESC

---List the lowest closing price for each ticker.
select top 1 Ticker, [Close] from trading_practice_dataset
order by [Close]; 

---Find the average trade volume per trader.
select Trader, avg(Volume) as avg_volume from trading_practice_dataset
group by Trader
order by avg_volume DESC

---Which desk executed the most trades?
select top 1 Desk, count(*) as no_of_trades from trading_practice_dataset
group by Desk
order by no_of_trades Desc

---Get the average high price per ticker, grouped by trader.
select trader, Ticker, AVG([High]) as avg_high_price  from trading_practice_dataset
group by Trader, Ticker
order by avg_high_price;

---time-Series / Window Functions
---11. Calculate the 7-day moving average closing price for each ticker.
select Date,Trader,Ticker, avg([Close]) over (partition by  Ticker order by Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS DAYS_7_MOVING_AVG 
from trading_practice_dataset
order by Ticker, Date

---Identify the highest 5-day rolling volume for each ticker.

WITH rolling_vol AS (
    SELECT 
        Ticker,
        [Date],
        SUM(Volume) OVER (
            PARTITION BY Ticker
            ORDER BY [Date]
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_5d_volume
    FROM trading_practice_dataset
)
SELECT Ticker, [Date], rolling_5d_volume
FROM rolling_vol r
WHERE rolling_5d_volume = (
    SELECT MAX(rolling_5d_volume)
    FROM rolling_vol r2
    WHERE r2.Ticker = r.Ticker
);
---Calculate the daily return percentage for each ticker.

select Ticker, [Close], [Open], (([Close] - [Open]) * 100) / [Open] as return_income from trading_practice_dataset
order by [Date];

---Find the most volatile ticker (highest std dev of returns).
select top 1 Ticker, STDEV((([Close] - [Open]) * 100) / [Open]) as Volatility from trading_practice_dataset
group by Ticker
order by Volatility Desc

---Identify the longest streak of daily gains for each ticker.
WITH gains AS (
    SELECT 
        Ticker,
        [Date],
        CASE WHEN [Close] > [Open] THEN 1 ELSE 0 END AS is_gain
    FROM trading_practice_dataset
),
streaks AS (
    SELECT 
        Ticker,
        [Date],
        is_gain,
        -- create a "group id" for each new streak of gains
        SUM(CASE WHEN is_gain = 0 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY Ticker ORDER BY [Date] ROWS UNBOUNDED PRECEDING) AS grp
    FROM gains
),
counted AS (
    SELECT 
        Ticker,
        grp,
        COUNT(*) AS streak_length
    FROM streaks
    WHERE is_gain = 1
    GROUP BY Ticker, grp
)
SELECT 
    Ticker,
    MAX(streak_length) AS longest_gain_streak
FROM counted
GROUP BY Ticker;

---Compare average volumes of traders across desks.
select Trader, Desk, AVG(Volume) as avg_volume from trading_practice_dataset
group by Trader, Desk
order by avg_volume

---17. Find the top trader by P&L (Close − Open).
--- top trader by Profit
select top 1 Trader, sum([Close] - [Open]) as total_Pro_loss from trading_practice_dataset
group by Trader
order by total_Pro_loss DESC

--- top trader by loss
select top 1 Trader, sum([Close] - [Open]) as total_Pro_loss from trading_practice_dataset
group by Trader
order by total_Pro_loss 
---Rank tickers by average high-low spread.
select Ticker, AVG([High] - [Low]) as avg_spread, RANK() over (order by avg([High] - [Low]) DESC) as spread_rank
from trading_practice_dataset
group by Ticker
order by spread_rank
---For each desk, find the most traded ticker.
select top 1 Desk, Ticker, count(*) as no_of_trades from trading_practice_dataset
group by Desk, Ticker
order by no_of_trades DESC

---Find traders who only traded in one ticker.
select Trader from trading_practice_dataset 
group by Trader
having COUNT(DISTINCT Ticker) = 1;


SELECT Trader
FROM trading_practice_dataset
GROUP BY Trader
HAVING COUNT(DISTINCT Ticker) = 1

SELECT Trader, MIN(Ticker) AS only_ticker
FROM trading_practice_dataset
GROUP BY Trader
HAVING COUNT(DISTINCT Ticker) = 1;

---Count trades per month per ticker.
select MONTH([Date]) as Month, Ticker, count(*) as no_of_trades
from trading_practice_dataset
group by MONTH([Date]), Ticker

---Average closing price per week for each ticker
select DATEPART(WEEK, [Date]) as week_no, Ticker, AVG([Close]) as avg_closing_price from trading_practice_dataset
group by DATEPART(WEEK, [Date]), Ticker

---Find max daily trading volume for each desk.
select Desk, Max(Volume) as max_daily_volume from trading_practice_dataset
group by Desk

---Calculate the proportion of trades per desk.
select Desk,
count(*) * 1.0/ (select count(*) from trading_practice_dataset) as proporation_of_trades
from trading_practice_dataset
group by Desk;

SELECT 
    Desk,
    COUNT(*) * 1.0 / (SELECT COUNT(*) FROM trading_practice_dataset) AS proportion_of_trades
FROM trading_practice_dataset
GROUP BY Desk;

---25. Group trades by ticker and desk → find avg returns.
select Desk, Ticker, AVG((([Close] - [Open]) * 100) / [Open]) as avg_returns from trading_practice_dataset 
group by Desk, Ticker

---Load this result into Tableau/Power BI → then pivot into a heatmap with Trader vs. Ticker and color = SUM(Volume).

SELECT 
    Trader,
    Ticker,
    SUM(Volume) AS total_volume
FROM trading_practice_dataset
GROUP BY Trader, Ticker
ORDER BY Trader, Ticker;
---Which ticker had the largest average daily profit opportunity (Close − Open)?
select top 1 Ticker, AVG([Close]-[Open]) avg_daily_profit from trading_practice_dataset
group by Ticker
order by avg_daily_profit DESC
---Estimate trader performance ranking by P&L.
select Trader,SUM([Close]-[Open]) as Profit_lost, RANK() over (order by sum([Close]-[Open]) DESC) as rankwise from trading_practice_dataset
group by Trader
