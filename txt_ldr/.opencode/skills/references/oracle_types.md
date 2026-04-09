# Oracle Type Mapping Reference

## VARCHAR2 length guidelines

| Pattern                        | Oracle type       |
|-------------------------------|-------------------|
| id, code, key (short)         | VARCHAR2(20)      |
| name, label, title            | VARCHAR2(255)     |
| email, mail                   | VARCHAR2(255)     |
| phone, tel, mobile, fax       | VARCHAR2(20)      |
| zip, postal, postcode         | VARCHAR2(10)      |
| url, link, href               | VARCHAR2(500)     |
| description, comment, note    | VARCHAR2(500) or CLOB |
| address line                  | VARCHAR2(200)     |
| country_code, currency_code   | CHAR(3)           |
| flag, active, enabled (0/1)   | CHAR(1)           |

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

## CLOB rules

Use CLOB when:
- max observed length > 2000 characters
- column name is: description, body, content, html, xml, json, payload, notes, comments, remarks
- column contains JSON or XML snippets

## Nullability rules

Force NOT NULL for:
- Any column ending in _id, _code, _key, _num
- Columns named: id, code, status, type, category, year, date
- Primary key candidates (single column with all unique non-null values)

## Index suggestions

Always suggest for:
- Columns ending in _id (foreign key candidate)
- Columns named status, type, category (low cardinality — bitmap index)
- Date columns used as partitioning or range filter candidates

Example:
```sql
CREATE INDEX IDX_SALES_FACT_YEAR ON SALES_FACT(YEAR);
CREATE BITMAP INDEX IDX_SALES_FACT_STATUS ON SALES_FACT(STATUS);
```