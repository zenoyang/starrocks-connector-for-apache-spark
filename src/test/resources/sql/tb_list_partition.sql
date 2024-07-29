CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id     BIGINT      NOT NULL COMMENT 'user ID',
    shop_id     BIGINT      NOT NULL COMMENT 'SHOP ID',
    city        VARCHAR(32) NOT NULL COMMENT 'city name',
    access_time BIGINT      NOT NULL COMMENT 'access timestamp'
) ENGINE = OLAP DUPLICATE KEY(user_id, shop_id)
PARTITION BY LIST (city) (
    PARTITION p_east VALUES IN ('shanghai', 'zhejiang', 'jiangsu'),
    PARTITION p_south VALUES IN ('guangdong', 'guangxi', 'yunnan'),
    PARTITION p_west VALUES IN ('xian', 'sicuan'),
    PARTITION p_north VALUES IN ('beijing', 'hebei', 'tianjin')
)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);