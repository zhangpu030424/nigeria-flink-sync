-- 源 user_order.product_id → 目标 product_id 映射表（可 JOIN）
-- P1 → 648；P2~P6 → 6481..6485
-- L1~L6 → 649..654；L7~L18 → 6551,6561,...,6661（654 之后后缀加 1）
-- 未知源码：LEFT JOIN 后 COALESCE(m.dst, src) 保持原样
--
-- 注意：勿 DROP 本表（application_order_lookup 等视图依赖）；用 TRUNCATE + 重灌

CREATE TABLE IF NOT EXISTS product_id_map (
    src VARCHAR(32) NOT NULL PRIMARY KEY,
    dst VARCHAR(32) NOT NULL
) COMMENT '源产品码→目标产品ID';

TRUNCATE TABLE product_id_map;

INSERT INTO product_id_map (src, dst) VALUES
('P1', '648'),
('P2', '6481'),
('P3', '6482'),
('P4', '6483'),
('P5', '6484'),
('P6', '6485'),
('L1', '649'),
('L2', '650'),
('L3', '651'),
('L4', '652'),
('L5', '653'),
('L6', '654'),
('L7', '6551'),
('L8', '6561'),
('L9', '6571'),
('L10', '6581'),
('L11', '6591'),
('L12', '6601'),
('L13', '6611'),
('L14', '6621'),
('L15', '6631'),
('L16', '6641'),
('L17', '6651'),
('L18', '6661');
