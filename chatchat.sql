-- ChatChat 建表脚本（W1-M05 冻结）——唯一事实源，全程禁用 GORM AutoMigrate。
-- 约定：utf8mb4/utf8mb4_0900_ai_ci；时间 DATETIME(3) 且 UTC 存储（应用 DSN loc=UTC，
-- DEFAULT CURRENT_TIMESTAMP(3) 仅为兜底）；主键 BIGINT UNSIGNED；无外键（清理顺序自由）。
-- 执行语境：目标库由执行方指定（compose initdb 于 MYSQL_DATABASE 上下文；测试经 DSN）。

DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `username`   VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '用户名（W1 可空；注册语义 W1-M06 冻结）',
  `password`   VARCHAR(100) NOT NULL COMMENT 'bcrypt 哈希，禁止明文',
  `phone`      VARCHAR(20)  NOT NULL COMMENT '手机号（大陆 11 位）',
  `avatar`     VARCHAR(255) NOT NULL DEFAULT '' COMMENT '头像 URL',
  `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户';

DROP TABLE IF EXISTS `user_device`;
CREATE TABLE `user_device` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id`       BIGINT UNSIGNED NOT NULL,
  `device_id`     VARCHAR(128) NOT NULL DEFAULT '',
  `platform`      VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '平台标识 ios/android/web',
  `last_login_at` DATETIME(3)  NULL,
  `created_at`    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_device` (`user_id`,`device_id`),
  KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户设备（ADR-0003：仅建表，无写入逻辑）';

DROP TABLE IF EXISTS `friend`;
CREATE TABLE `friend` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id`    BIGINT UNSIGNED NOT NULL,
  `friend_id`  BIGINT UNSIGNED NOT NULL,
  `status`     TINYINT      NOT NULL DEFAULT 0 COMMENT '0=申请中 1=已好友 2=已拉黑',
  `remark`     VARCHAR(64)  NOT NULL DEFAULT '',
  `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_friend` (`user_id`,`friend_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='好友关系（W3 使用）';

DROP TABLE IF EXISTS `group_info`;
CREATE TABLE `group_info` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name`       VARCHAR(64)  NOT NULL,
  `owner_id`   BIGINT UNSIGNED NOT NULL,
  `max_member` INT          NOT NULL DEFAULT 500 COMMENT '成员上限（spec §8.2 硬性指标）',
  `hot_level`  TINYINT      NOT NULL DEFAULT 0 COMMENT '热点等级（0=普通，1=热点；>1 预留；W4-M01 D6）',
  `status`     TINYINT      NOT NULL DEFAULT 0 COMMENT '0=正常 1=解散',
  `announcement` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '群公告（W3-M01 增列；≤512 字符，pkg/global.GroupAnnouncementMaxRunes）',
  `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_owner` (`owner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='群组（W3 使用）';

DROP TABLE IF EXISTS `group_member`;
CREATE TABLE `group_member` (
  `id`        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `group_id`  BIGINT UNSIGNED NOT NULL,
  `user_id`   BIGINT UNSIGNED NOT NULL,
  `role`      TINYINT     NOT NULL DEFAULT 0 COMMENT '0=成员 1=管理员 2=群主',
  `muted_until` DATETIME(3) NULL COMMENT '禁言到期（W3-M01 增列；NULL=未禁言；发言检查点 W3-M04，spec §8.2）',
  `joined_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_user` (`group_id`,`user_id`),
  KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='群成员（W3 使用）';

DROP TABLE IF EXISTS `hot_group_stat`;
CREATE TABLE `hot_group_stat` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `group_id`   BIGINT UNSIGNED NOT NULL COMMENT '群ID',
  `window_ts`  DATETIME(3)     NOT NULL COMMENT '统计窗口起点（UTC，对齐聚合周期与桶边界，W4-M01 D9）',
  `msg_count`  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '窗口内消息数（时间桶聚合，spec §7.1/§10）',
  `created_at` DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间（UTC 毫秒）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_window` (`group_id`, `window_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='热点群打标统计（W4-M01 新增，spec §10）';

DROP TABLE IF EXISTS `session`;
CREATE TABLE `session` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `conversation_id` VARCHAR(128) NOT NULL COMMENT '单聊 u1|u2 排序拼接；群聊 g_{groupId}（spec §5）',
  `type`            TINYINT     NOT NULL COMMENT '0=单聊 1=群聊',
  `members`         JSON        NOT NULL COMMENT '成员 uid 数组（JSON 格式 W2 冻结）',
  `created_at`      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at`      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_conversation` (`conversation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='会话（W2 使用）';

DROP TABLE IF EXISTS `message`;
CREATE TABLE `message` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `msg_id`          VARCHAR(64)  NOT NULL COMMENT '全局唯一消息ID（服务端 crypto/rand 16 字节 hex，32 字符）',
  `conversation_id` VARCHAR(128) NOT NULL COMMENT '会话ID（单聊: u1|u2 数值升序拼接，分隔符|；群聊: g_{groupId}，spec §5）',
  `from_uid`        BIGINT UNSIGNED NOT NULL COMMENT '发送者 userId（服务端鉴权覆写，不信任客户端）',
  `to_uid`          BIGINT UNSIGNED NOT NULL COMMENT '接收者 userId',
  `content_type`    INT          NOT NULL DEFAULT 1 COMMENT '内容类型 1文本..8自定义（对齐 pkg/protocol ContentType；0 视为缺省按 1 落库）',
  `content`         TEXT         NOT NULL COMMENT '消息内容（文本/JSON 扩展；上限 32768 字节，pkg/global.MessageContentMaxBytes）',
  `url`             VARCHAR(512) NOT NULL DEFAULT '' COMMENT '媒体资源地址（大文件走 HTTPS 上传换 URL，spec §5）',
  `msg_seq`         BIGINT UNSIGNED NOT NULL COMMENT '会话内递增序号（Redis conv:{conversationId}:seq INCR，spec §10）',
  `created_at`      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_msg_id` (`msg_id`),
  UNIQUE KEY `uk_conv_seq` (`conversation_id`, `msg_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='消息（W2-M03 追加，spec §10；from_uid/to_uid 规避保留字 from，语义对齐 ChatMessage.from/to）';

DROP TABLE IF EXISTS `inbox_delivery`;
CREATE TABLE `inbox_delivery` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id`         BIGINT UNSIGNED NOT NULL COMMENT '信箱归属用户',
  `conversation_id` VARCHAR(128)    NOT NULL COMMENT '会话ID（单聊 u1|u2；群聊 g_{groupId}，spec §5）',
  `msg_id`          VARCHAR(64)     NOT NULL COMMENT '消息ID（关联 trace/排障；幂等主键为 uk_user_conv_seq，ADR-0007 强键口径）',
  `msg_seq`         BIGINT UNSIGNED NOT NULL COMMENT '会话内序号（去重/排序核心）',
  `created_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_conv_seq` (`user_id`, `conversation_id`, `msg_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='信箱投递记录（W3-M05 写扩散，spec §6.1/§6.4；读侧消费归 W3-M06）';

DROP TABLE IF EXISTS `inbox_cursor`;
CREATE TABLE `inbox_cursor` (
  `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `user_id`         BIGINT UNSIGNED NOT NULL COMMENT '信箱归属用户',
  `conversation_id` VARCHAR(128)    NOT NULL COMMENT '会话ID（单聊 u1|u2；群聊 g_{groupId}，spec §5）',
  `max_synced_seq`  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '拉取侧游标：已同步最大 seq（W2-M05 推进）',
  `max_recv_seq`    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '推送侧游标：已确认接收最大 seq（ACK 推进，spec §6.2/§10）',
  `created_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间（UTC 毫秒）',
  `updated_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间（UTC 毫秒）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_conv` (`user_id`, `conversation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='信箱游标（W2-M04 追加，spec §10）';

DROP TABLE IF EXISTS `group_timeline`;
CREATE TABLE `group_timeline` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `group_id`   BIGINT UNSIGNED NOT NULL COMMENT '群ID（g_{groupId} 会话，热点读扩散）',
  `msg_seq`    BIGINT UNSIGNED NOT NULL COMMENT '会话内序号（与 message.msg_seq 同源，排序/去重核心）',
  `msg_id`     VARCHAR(64)     NOT NULL COMMENT '消息ID（引用 message 表；幂等主键为 uk_group_seq，ADR-0007 强键口径）',
  `created_at` DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间（UTC 毫秒）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_seq` (`group_id`, `msg_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='热点群 Timeline（W4-M02 读扩散写路径，spec §7.2/§10；读侧归 M03，分表分桶下月）';
