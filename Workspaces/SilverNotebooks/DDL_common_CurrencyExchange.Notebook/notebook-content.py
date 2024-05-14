# CELL ********************

spark.sql('''
CREATE TABLE IF NOT EXISTS silver_common_CurrencyExchange (
    Date DATE,
    FromCurrency STRING,
    ToCurrency STRING,
    Exchange DOUBLE
)
USING DELTA
'''
)
