from sqlalchemy import text
from config import get_engine, logger

try:
    engine = get_engine()

    with engine.connect() as conn:

        # 1. Daily Returns
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_daily_returns AS
            SELECT
                s.date,
                s.ticker,
                d.company_name,
                d.sector,
                s.close,
                LAG(s.close) OVER (PARTITION BY s.ticker ORDER BY s.date) AS prev_close,
                ROUND(
                    ((s.close - LAG(s.close) OVER (PARTITION BY s.ticker ORDER BY s.date))
                    / LAG(s.close) OVER (PARTITION BY s.ticker ORDER BY s.date) * 100)::numeric
                , 2) AS daily_return_pct
            FROM silver_stock_prices s
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
        """))
        logger.info("gold_daily_returns — OK")

        # 2. Moving Averages
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_moving_average AS
            SELECT
                s.date,
                s.ticker,
                d.company_name,
                s.close,
                ROUND(AVG(s.close) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )::numeric, 2) AS ma_30d,
                ROUND(AVG(s.close) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
                )::numeric, 2) AS ma_90d
            FROM silver_stock_prices s
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
        """))
        logger.info("gold_moving_average — OK")

        # 3. Performance Summary
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_performance_summary AS
            WITH latest AS (
                SELECT ticker, close, date,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM silver_stock_prices WHERE status = 'valid'
            ),
            start_of_year AS (
                SELECT ticker, close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC) AS rn
                FROM silver_stock_prices
                WHERE status = 'valid' AND date >= DATE_TRUNC('year', CURRENT_DATE)
            ),
            one_month_ago AS (
                SELECT ticker, close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC) AS rn
                FROM silver_stock_prices
                WHERE status = 'valid' AND date >= CURRENT_DATE - INTERVAL '1 month'
            ),
            three_months_ago AS (
                SELECT ticker, close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC) AS rn
                FROM silver_stock_prices
                WHERE status = 'valid' AND date >= CURRENT_DATE - INTERVAL '3 months'
            )
            SELECT
                l.ticker,
                d.company_name,
                d.sector,
                ROUND(l.close::numeric, 2) AS current_price,
                ROUND(((l.close - sy.close) / sy.close * 100)::numeric, 2) AS ytd_return_pct,
                ROUND(((l.close - om.close) / om.close * 100)::numeric, 2) AS one_month_return_pct,
                ROUND(((l.close - tm.close) / tm.close * 100)::numeric, 2) AS three_month_return_pct
            FROM latest l
            JOIN dim_ticker d ON l.ticker = d.ticker
            LEFT JOIN start_of_year sy ON l.ticker = sy.ticker AND sy.rn = 1
            LEFT JOIN one_month_ago om ON l.ticker = om.ticker AND om.rn = 1
            LEFT JOIN three_months_ago tm ON l.ticker = tm.ticker AND tm.rn = 1
            WHERE l.rn = 1
        """))
        logger.info("gold_performance_summary — OK")

        # 4. Cumulative Returns
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_cumulative_returns AS
            WITH first_close AS (
                SELECT ticker, MIN(date) AS first_date
                FROM silver_stock_prices WHERE status = 'valid'
                GROUP BY ticker
            ),
            base AS (
                SELECT s.ticker, s.close AS base_close
                FROM silver_stock_prices s
                JOIN first_close f ON s.ticker = f.ticker AND s.date = f.first_date
            )
            SELECT
                s.date,
                s.ticker,
                d.company_name,
                d.sector,
                s.close,
                ROUND(((s.close - b.base_close) / b.base_close * 100)::numeric, 2) AS cumulative_return_pct
            FROM silver_stock_prices s
            JOIN base b ON s.ticker = b.ticker
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
        """))
        logger.info("gold_cumulative_returns — OK")

        # 5. Volatility
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_volatility AS
            SELECT
                s.date,
                s.ticker,
                d.company_name,
                d.sector,
                ROUND(STDDEV(s.close) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )::numeric, 2) AS volatility_30d,
                ROUND(STDDEV(s.close) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
                )::numeric, 2) AS volatility_90d,
                ROUND(STDDEV(s.close) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
                )::numeric, 2) AS volatility_180d
            FROM silver_stock_prices s
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
        """))
        logger.info("gold_volatility — OK")

        # 6. Drawdown
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_drawdown AS
            SELECT
                s.date,
                s.ticker,
                d.company_name,
                s.close,
                MAX(s.close) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS peak_price,
                ROUND(
                    ((s.close - MAX(s.close) OVER (
                        PARTITION BY s.ticker ORDER BY s.date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )) / MAX(s.close) OVER (
                        PARTITION BY s.ticker ORDER BY s.date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) * 100)::numeric
                , 2) AS drawdown_pct
            FROM silver_stock_prices s
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
        """))
        logger.info("gold_drawdown — OK")

        # 7. Monthly Volume
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_monthly_volume AS
            SELECT
                DATE_TRUNC('month', s.date) AS month,
                s.ticker,
                d.company_name,
                d.sector,
                ROUND(AVG(s.volume)::numeric, 0) AS avg_daily_volume,
                SUM(s.volume) AS total_volume
            FROM silver_stock_prices s
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
            GROUP BY DATE_TRUNC('month', s.date), s.ticker, d.company_name, d.sector
            ORDER BY month, s.ticker
        """))
        logger.info("gold_monthly_volume — OK")

        # 8. Volume vs Price
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_volume_vs_price AS
            SELECT
                s.date,
                s.ticker,
                d.company_name,
                s.close,
                s.volume,
                ROUND(AVG(s.volume) OVER (
                    PARTITION BY s.ticker ORDER BY s.date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )::numeric, 0) AS avg_volume_30d,
                CASE
                    WHEN s.volume > AVG(s.volume) OVER (
                        PARTITION BY s.ticker ORDER BY s.date
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) * 1.5 THEN 'high_volume'
                    ELSE 'normal_volume'
                END AS volume_signal
            FROM silver_stock_prices s
            JOIN dim_ticker d ON s.ticker = d.ticker
            WHERE s.status = 'valid'
        """))
        logger.info("gold_volume_vs_price — OK")

        # 9. Sector Comparison
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_sector_comparison AS
            WITH daily AS (
                SELECT
                    s.date,
                    d.sector,
                    AVG(s.close) AS avg_close
                FROM silver_stock_prices s
                JOIN dim_ticker d ON s.ticker = d.ticker
                WHERE s.status = 'valid'
                GROUP BY s.date, d.sector
            ),
            first_by_sector AS (
                SELECT sector, MIN(date) AS first_date
                FROM daily GROUP BY sector
            ),
            base AS (
                SELECT d.sector, d.avg_close AS base_close
                FROM daily d
                JOIN first_by_sector f ON d.sector = f.sector AND d.date = f.first_date
            )
            SELECT
                d.date,
                d.sector,
                ROUND(d.avg_close::numeric, 2) AS avg_close,
                ROUND(((d.avg_close - b.base_close) / b.base_close * 100)::numeric, 2) AS sector_return_pct
            FROM daily d
            JOIN base b ON d.sector = b.sector
            ORDER BY d.date, d.sector
        """))
        logger.info("gold_sector_comparison — OK")

        # 10. Correlation
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_correlation AS
            WITH returns AS (
                SELECT
                    ticker,
                    date,
                    ((close - LAG(close) OVER (PARTITION BY ticker ORDER BY date))
                    / LAG(close) OVER (PARTITION BY ticker ORDER BY date)) AS daily_return
                FROM silver_stock_prices
                WHERE status = 'valid'
            ),
            pairs AS (
                SELECT
                    a.ticker AS ticker_a,
                    b.ticker AS ticker_b,
                    a.date,
                    a.daily_return AS return_a,
                    b.daily_return AS return_b
                FROM returns a
                JOIN returns b
                ON a.date = b.date
                AND a.ticker < b.ticker
                WHERE a.daily_return IS NOT NULL
                AND b.daily_return IS NOT NULL
            )
            SELECT
                ticker_a,
                ticker_b,
                ROUND(CORR(return_a, return_b)::numeric, 4) AS correlation
            FROM pairs
            GROUP BY ticker_a, ticker_b
        """))
        logger.info("gold_correlation — OK")

        # 11. Best/Worst Performers
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold_best_worst_performers AS
            WITH latest AS (
                SELECT ticker, close AS latest_close
                FROM silver_stock_prices
                WHERE status = 'valid'
                AND date = (SELECT MAX(date) FROM silver_stock_prices WHERE status = 'valid')
            ),
            close_30d_ago AS (
                SELECT DISTINCT ON (ticker) ticker, close AS close_30d
                FROM silver_stock_prices
                WHERE status = 'valid' AND date <= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY ticker, date DESC
            ),
            close_90d_ago AS (
                SELECT DISTINCT ON (ticker) ticker, close AS close_90d
                FROM silver_stock_prices
                WHERE status = 'valid' AND date <= CURRENT_DATE - INTERVAL '90 days'
                ORDER BY ticker, date DESC
            ),
            close_180d_ago AS (
                SELECT DISTINCT ON (ticker) ticker, close AS close_180d
                FROM silver_stock_prices
                WHERE status = 'valid' AND date <= CURRENT_DATE - INTERVAL '180 days'
                ORDER BY ticker, date DESC
            )
            SELECT
                l.ticker,
                d.company_name,
                d.sector,
                ROUND(l.latest_close::numeric, 2) AS latest_close,
                ROUND(c30.close_30d::numeric, 2) AS close_30d,
                ROUND(c90.close_90d::numeric, 2) AS close_90d,
                ROUND(c180.close_180d::numeric, 2) AS close_180d,
                ROUND(((l.latest_close - c30.close_30d) / c30.close_30d * 100)::numeric, 2) AS return_30d_pct,
                ROUND(((l.latest_close - c90.close_90d) / c90.close_90d * 100)::numeric, 2) AS return_90d_pct,
                ROUND(((l.latest_close - c180.close_180d) / c180.close_180d * 100)::numeric, 2) AS return_180d_pct,
                RANK() OVER (ORDER BY ((l.latest_close - c30.close_30d) / c30.close_30d) DESC) AS rank_best
            FROM latest l
            JOIN dim_ticker d ON l.ticker = d.ticker
            LEFT JOIN close_30d_ago c30 ON l.ticker = c30.ticker
            LEFT JOIN close_90d_ago c90 ON l.ticker = c90.ticker
            LEFT JOIN close_180d_ago c180 ON l.ticker = c180.ticker
            ORDER BY return_30d_pct DESC
        """))
        logger.info("gold_best_worst_performers — OK")

        conn.commit()
        logger.info("All 11 gold views created successfully.")

except Exception:
    logger.exception("load_gold.py failed")
    raise