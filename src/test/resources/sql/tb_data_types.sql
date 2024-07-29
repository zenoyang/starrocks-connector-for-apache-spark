CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    boolean_value1       BOOLEAN,
    boolean_value2       BOOLEAN,
    boolean_null_value   BOOLEAN,
    tinyint_value        TINYINT,
    tinyint_null_value   TINYINT,
    smallint_value       SMALLINT,
    smallint_null_value  SMALLINT,
    int_value            INT,
    int_null_value       INT,
    bigint_value         BIGINT,
    bigint_null_value    BIGINT,
    largeint_lower LARGEINT,
    largeint_upper LARGEINT,
    largeint_null_value LARGEINT,
    float_value          FLOAT,
    float_null_value     FLOAT,
    double_value         DOUBLE,
    double_null_value    DOUBLE,
    decimal_value1       DECIMAL(20, 10),
    decimal_value2       DECIMAL(32, 10),
    decimal_value3       DECIMAL(10, 6),
    decimal_null_value3  DECIMAL(10, 6),
    char_value1          CHAR(16),
    char_value2          CHAR(32),
    char_null_value      CHAR(32),
    varchar_value1       CHAR(16),
    varchar_value2       CHAR(32),
    varchar_null_value   CHAR(32),
    string_value         STRING,
    string_null_value    STRING,
    date_value           DATE,
    date_null_value      DATE,
    datetime_value       DATETIME,
    datetime_null_value  DATETIME,
    json_object_value    JSON,
    json_array_value     JSON,
    json_null_value      JSON
) ENGINE = OLAP DUPLICATE KEY (boolean_value1, boolean_value2)
DISTRIBUTED BY HASH(boolean_value1, boolean_value2) BUCKETS 4
PROPERTIES(
    "replication_num" = "1"
);