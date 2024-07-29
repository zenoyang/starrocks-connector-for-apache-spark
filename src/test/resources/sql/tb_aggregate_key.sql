CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id BIGINT   NOT NULL COMMENT 'user ID',
    site_id CHAR(32) NOT NULL COMMENT 'site ID',
    pv      BIGINT SUM DEFAULT "0" COMMENT 'total page views'
) ENGINE = OLAP AGGREGATE KEY(user_id, site_id)
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
    "replication_num" = "1"
);