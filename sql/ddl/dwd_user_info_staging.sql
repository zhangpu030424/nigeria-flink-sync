-- DWD 中间库 ng_migration_dwd（与 platform_db 同 MySQL 实例，不同 database）
-- 库由 run-ng-user-info-gpt-dwd.sh 自动 CREATE DATABASE IF NOT EXISTS
-- 表 DDL: mysql -h... -u... -p ng_migration_dwd < sql/ddl/dwd_user_info_staging.sql

CREATE TABLE IF NOT EXISTS dwd_user_base (
    user_id BIGINT UNSIGNED NOT NULL,
    app_id INT UNSIGNED NOT NULL,
    mobile VARCHAR(191) NOT NULL,
    device_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    is_cancel TINYINT NOT NULL DEFAULT 0,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_time BIGINT UNSIGNED NOT NULL DEFAULT 0,
    reg_time BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id),
    KEY idx_app_mobile (app_id, mobile),
    KEY idx_device_id (device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dwd_latest_user_data (
    userId BIGINT UNSIGNED NOT NULL,
    id BIGINT UNSIGNED NOT NULL,
    bvn VARCHAR(30) NOT NULL DEFAULT '',
    firstName VARCHAR(30) NOT NULL DEFAULT '',
    middleName VARCHAR(30) NOT NULL DEFAULT '',
    lastName VARCHAR(30) NOT NULL DEFAULT '',
    email VARCHAR(255) NOT NULL DEFAULT '',
    birthday VARCHAR(30) NOT NULL DEFAULT '',
    gender VARCHAR(10) NOT NULL DEFAULT '',
    addressState VARCHAR(30) NOT NULL DEFAULT '',
    addressDistrict VARCHAR(30) NOT NULL DEFAULT '',
    address VARCHAR(255) NOT NULL DEFAULT '',
    company VARCHAR(255) NOT NULL DEFAULT '',
    education VARCHAR(10) NOT NULL DEFAULT '',
    marital VARCHAR(10) NOT NULL DEFAULT '',
    profession VARCHAR(10) NOT NULL DEFAULT '',
    salary VARCHAR(10) NOT NULL DEFAULT '',
    numberOfChildren VARCHAR(10) NOT NULL DEFAULT '',
    payCycle VARCHAR(10) NOT NULL DEFAULT '',
    salaryDay VARCHAR(10) NOT NULL DEFAULT '',
    emergencyContact VARCHAR(255) NOT NULL DEFAULT '',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dwd_latest_user_password (
    appId INT NOT NULL,
    mobile VARCHAR(30) NOT NULL,
    id BIGINT NOT NULL,
    password VARCHAR(50) NOT NULL DEFAULT '',
    created TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (appId, mobile)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dwd_latest_device_channel (
    deviceId BIGINT NOT NULL,
    id BIGINT NOT NULL,
    channel VARCHAR(30) NOT NULL DEFAULT '',
    PRIMARY KEY (deviceId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dwd_latest_user_reg_ip (
    userId BIGINT UNSIGNED NOT NULL,
    id BIGINT UNSIGNED NOT NULL,
    ip VARCHAR(255) NOT NULL DEFAULT '',
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (userId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
