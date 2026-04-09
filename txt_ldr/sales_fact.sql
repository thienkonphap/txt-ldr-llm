CREATE TABLE DWH.SALES_FACT
(
  SERIES_REFERENCE              VARCHAR2(20) NOT NULL,
  PERIOD                        NUMBER(15,2) NOT NULL,
  DATA_VALUE                    NUMBER(18,6) NOT NULL,
  STATUS                        VARCHAR2(10) NOT NULL,
  UNITS                         VARCHAR2(10) NOT NULL,
  SUBJECT                       VARCHAR2(50) NOT NULL,
  GROUP                         VARCHAR2(100) NOT NULL,
  SERIES_TITLE_1                VARCHAR2(50) NOT NULL,
  SERIES_TITLE_2                VARCHAR2(255),
  SERIES_TITLE_3                VARCHAR2(255),
  SERIES_TITLE_4                VARCHAR2(255),
  SERIES_TITLE_5                VARCHAR2(255)
);