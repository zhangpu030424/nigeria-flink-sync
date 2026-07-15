create table application
(
    application_no             char(36)                     not null comment '申请单号',
    mobile                     char(28)                     not null comment '手机号',
    bid                        varchar(10)                  not null comment '业务线',
    app_id                     int unsigned                 not null comment 'App ID',
    app_version                char(36)                     null,
    user_id                    bigint unsigned              not null,
    group_user_id              bigint unsigned              not null,
    sn                         char(36)                     not null comment '序列号',
    is_test                    tinyint unsigned default '0' not null comment '是否测试（0-否，1-是）',
    is_first_apply             tinyint unsigned default '0' not null comment '是否首次申请（0-否，1-是）',
    is_auto_apply              tinyint unsigned default '0' not null,
    id_number                  char(28)                     not null comment '身份证号',
    gaid_idfa                  char(36)                     null comment 'GAID或IDFA',
    device_uuid                char(36)                     not null comment '设备UUID',
    session_id                 char(36)                     null comment '会话ID',
    bank_code                  varchar(36)                  not null comment '银行代码',
    bank_account_name          varchar(128)                 not null comment '开户名',
    bank_account_number        char(28)                     not null comment '银行卡号',
    product_id                 varchar(64)                  not null comment '产品ID',
    product_scheme_id          varchar(64)                  not null comment '产品方案ID',
    product_calculator_version varchar(32)                  not null comment '产品计算ID',
    repay_calculator_version   varchar(32)                  not null comment '还款计算版本',
    rollover_calculator_version varchar(32)                 not null comment '展期计算版本',
    product_scheme_param       json                         null comment '参数',
    term                       int unsigned                 not null comment '期限（天）',
    periods                    int unsigned     default '1' not null comment '期数',
    repayment_method           tinyint          default 1   not null comment '还款方式：1-到期还本付息，2-蝌蚪，3-等额本息，4-等额本金，5-先息后本',
    repayment_plan             json                         not null comment '还款计划（试算）',
    credit_limit               bigint unsigned  default '0' not null comment '授信金额',
    loan_amount                bigint unsigned              not null comment '申请金额',
    principal                  bigint unsigned              not null comment '本金',
    total_amount               bigint unsigned  default '0' not null comment '应还金额',
    disbursed_amount           bigint unsigned  default '0' not null comment '实际放款金额',
    created_time               bigint unsigned              not null comment '创建时间(收到申请时服务器时间)  毫秒',
    submited_time              bigint unsigned              null comment '提交风控审核的时间  毫秒',
    reviewed_time              bigint unsigned              null comment '审核完成时间 毫秒',
    disbursed_time             bigint unsigned              null comment '放款时间 毫秒',
    last_paid_time             bigint unsigned              null comment '最后一次还款时间  毫秒',
    paid_off_time              bigint unsigned              null comment '结清时间  毫秒',
    lock_expire_time           bigint unsigned              null comment '申请锁定过期时间  毫秒',
    due_date                   date                         null comment '到期日期',
    due_date_final             date                         not null comment '最终到期日期(叠加宽限期)',
    status                     tinyint                      not null comment '状态',
    primary key (mobile, group_user_id, sn)
)
    comment '申请订单表' partition by key (`mobile`) partitions 16;

create index idx_ts
    on application (created_time);

create table id_mapping
(
    id         varchar(36)                                                                     not null,
    app_id     int unsigned                                                                    not null,
    mapping_id varchar(36)                                                                     not null,
    type       enum ('mobile', 'gaid_idfa', 'device_uuid', 'bank_account', 'id_number', 'id2') not null comment 'mapping_id 的类型',
    event_time bigint unsigned                                                                 not null,
    primary key (id, app_id, mapping_id)
)
    partition by key (`id`) partitions 64;

create table loan
(
    loan_no          char(36)                     not null comment '还款计划编号(格式:MX0625-672912921221-0100)',
    application_no   char(36)                     not null comment '申请单号',
    period tinyint unsigned default '1' not null comment '期序',
    roll_sequence    tinyint unsigned default '0' not null comment '展期序号',
    start_date       date                         not null comment '计划开始日期',
    due_date         date                         not null comment '到期日期',
    due_date_final   date                         not null comment '最终到期日期(叠加宽限期)',
    principal        bigint unsigned              not null comment '应还本金',
    interest         bigint unsigned              not null comment '应还利息',
    admin_fee        bigint unsigned              not null comment '应还管理费',
    roll_fee         bigint           default 0   not null comment '应还展期服务费',
    penalty_amount   bigint unsigned              not null comment '应还罚息',
    reduction_amount bigint unsigned              not null comment '已减免金额',
    total_amount     bigint unsigned              not null comment '应还总额',
    paid_amount      bigint unsigned  default '0' not null comment '结清已还金额 合计',
    roll_paid_amount bigint           default 0   not null comment '展期已还金额 合计',
    paid_time        bigint unsigned              null comment '最后一次还款时间毫秒',
    paid_off_date    date                         null comment '结清日期',
    created_time     bigint unsigned              not null comment '创建时间毫秒',
    status           tinyint          default 0   not null,
    primary key (application_no, period, roll_sequence)
)
    comment '还款计划表' partition by key (`application_no`) partitions 16;

create index idx_loan_no
    on loan (loan_no);

create index idx_status
    on loan (status);

create table user
(
    user_id         bigint unsigned                           not null,
    app_id          int unsigned                              not null comment 'App ID',
    group_user_id   bigint unsigned                           not null comment 'App Group User ID',
    info_user_id    bigint unsigned                           not null,
    mobile          char(28)                                  not null comment '手机号',
    closed_time     bigint unsigned default '0'               not null comment '注销时间, 0表示未注销',
    reg_device_uuid varchar(191)                              not null comment '注册设备编号',
    reg_time        bigint unsigned                           not null comment '注册的时间戳',
    test_flag       tinyint(1)      default 0                 not null comment '是否为测试账号 2 GP 1 是 0 否',
    utm_source      varchar(191)                              null,
    utm_medium      varchar(191)                              null,
    utm_campaign    varchar(191)                              null,
    utm_content     varchar(191)                              null,
    utm_term        varchar(191)                              null,
    campaign_id     varchar(191)                              null,
    ad_group_id     varchar(191)                              null,
    advertiser_id   varchar(191)                              null,
    created_at      timestamp       default CURRENT_TIMESTAMP not null,
    updated_at      timestamp       default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    primary key (mobile, app_id, closed_time)
)
    comment '用户' partition by key (`mobile`) partitions 16;

create table user_bankcard
(
    id                  bigint                                     not null,
    group_user_id       bigint unsigned                            not null comment 'App Group User ID',
    bank_code           varchar(64)                                not null comment '银行代码',
    bank_account_number varchar(64)                                not null comment '银行卡号',
    is_default          tinyint unsigned default '0'               not null comment '是否默认卡',
    created_at          timestamp        default CURRENT_TIMESTAMP not null,
    updated_at          timestamp        default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    primary key (group_user_id, bank_account_number)
)
    comment '用户银行卡' partition by key (`group_user_id`) partitions 16 row_format = DYNAMIC;

create table user_info
(
    user_id    bigint unsigned                        not null
        primary key,
    id_number  char(28)     default ''                not null comment '证件号',
    full_name  varchar(255) default ''                not null comment '用户的姓名',
    password   varchar(191) default ''                not null comment '密码',
    live_image varchar(191) default ''                not null comment '最佳人脸照片url',
    id_card    varchar(191) default ''                not null,
    info       json                                   not null comment '数据',
    created_at timestamp    default CURRENT_TIMESTAMP not null,
    updated_at timestamp    default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP
)
    comment '用户信息' partition by key (`user_id`) partitions 16;

create table user_product
(
    group_user_id    bigint unsigned                            not null,
    product_id       char(32)                                   not null comment '产品ID',
    schemes          json                                       not null comment '产品方案',
    is_open          tinyint unsigned default '0'               not null comment '是否开放',
    credit_amount    bigint unsigned  default '0'               not null comment '授信金额',
    unpaid_amount    bigint unsigned  default '0'               not null comment '在贷金额',
    locked_amount    bigint unsigned  default '0'               not null comment '锁定金额',
    available_amount bigint unsigned  default '0'               not null comment '可用金额',
    updated_at       timestamp        default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    created_at       timestamp        default CURRENT_TIMESTAMP not null,
    primary key (group_user_id, product_id)
)
    comment '用户产品' partition by key (`group_user_id`) partitions 16;

