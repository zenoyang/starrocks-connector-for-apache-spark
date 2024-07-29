CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    key_value            BIGINT,
    binary_null_value    BINARY,
    binary_value         BINARY,
    varbinary_null_value VARBINARY(128),
    varbinary_value      VARBINARY(128)
) ENGINE = OLAP DUPLICATE KEY(key_value)
DISTRIBUTED BY HASH(key_value) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);