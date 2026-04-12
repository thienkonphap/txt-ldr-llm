# Oracle Type Mapping Reference

## VARCHAR2 length guidelines

| Pattern                                        | Oracle type       |
|-----------------------------------------------|-------------------|
| id, code, key (short)                         | VARCHAR2(20)      |
| name, label, title                            | VARCHAR2(255)     |
| email, mail                                   | VARCHAR2(255)     |
| phone, tel, mobile, fax                       | VARCHAR2(20)      |
| zip, postal, postcode                         | VARCHAR2(10)      |
| url, link, href                               | VARCHAR2(500)     |
| description, comment, note, text, remarks     | VARCHAR2(4000)    |
| address line                                  | VARCHAR2(200)     |
| country_code, currency_code                   | CHAR(3)           |
| flag, active, enabled (0/1)                   | CHAR(1)           |

### Financial market identifiers

| Pattern                                        | Oracle type       | Notes                          |
|-----------------------------------------------|-------------------|-------------------------------|
| isin                                          | CHAR(12)          | ISO 6166 — always 12 chars    |
| mic, market_id, market_code                   | CHAR(4)           | ISO 10383 — 4 chars           |
| currency, ccy, currency_code                  | CHAR(3)           | ISO 4217 — 3 chars            |
| ticker, symbol, ric                           | VARCHAR2(20)      | Variable length               |
| sedol                                         | CHAR(7)           | 7 chars                       |
| cusip                                         | CHAR(9)           | 9 chars                       |
| figi                                          | CHAR(12)          | 12 chars                      |
| lei                                           | CHAR(20)          | ISO 17442 — 20 chars          |
| exchange_code, exchange                       | VARCHAR2(10)      |                               |
| asset_class, instrument_type, product_type   | VARCHAR2(50)      |                               |
| price, bid, ask, open, high, low, close       | NUMBER(18,8)      | High precision for FX/crypto  |
| volume, qty, quantity, shares                 | NUMBER(18,0)      | Integer quantities            |
| notional, amount, nominal                     | NUMBER(18,2)      | Standard monetary             |
| yield, rate, spread, bps                      | NUMBER(10,6)      | Basis points / rates          |
| market_cap, nav, aum                          | NUMBER(20,2)      | Large monetary values         |

## NUMBER precision guidelines

| Pattern                        | Oracle type       |
|-------------------------------|-------------------|
| year (4-digit)                | NUMBER(4)         |
| age, count, qty, quantity     | NUMBER(10)        |
| id (integer surrogate key)    | NUMBER(19)        |
| price, amount, total, value   | NUMBER(15,2)      |
| rate, ratio, percentage       | NUMBER(8,4)       |
| latitude, longitude           | NUMBER(10,7)      |
| weight, height, distance      | NUMBER(10,3)      |

## DATE vs TIMESTAMP

- Use DATE when time component is not present or not needed
- Use TIMESTAMP when microseconds or timezone matter
- Common SQLLDR masks:

| Format in CSV           | SQLLDR mask                  |
|------------------------|------------------------------|
| 2024-01-31             | YYYY-MM-DD                   |
| 31/01/2024             | DD/MM/YYYY                   |
| 31-JAN-2024            | DD-MON-YYYY                  |
| 2024-01-31 14:30:00    | YYYY-MM-DD HH24:MI:SS        |
| 2024-01-31T14:30:00    | YYYY-MM-DD"T"HH24:MI:SS      |
| 31/01/2024 14:30       | DD/MM/YYYY HH24:MI           |