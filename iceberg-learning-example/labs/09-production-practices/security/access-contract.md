# 最小权限访问契约

Iceberg 没有跨 REST Catalog、Spark、对象存储统一执行的 `GRANT` SQL 方言。权限通常由
Catalog 服务、查询引擎身份认证及对象存储策略共同决定。因此本实验提供可验证的行为
契约，不假设当前 REST Catalog 支持某个特定 RBAC 实现。

当前 Fixture 已于 2026-09-07 在外部环境完成手动验证。后续更换 Catalog、认证方式、
对象存储策略或运行身份后，必须重新执行本契约；具体身份配置与拒绝详情保留在本地验证
记录中，不提交凭据或敏感环境信息。

| 身份 | Catalog 权限 | 表 Location 的对象存储权限 | 本实验预期 |
| --- | --- | --- | --- |
| `reader` | 加载 `learning_security.orders_access_fixture` 及所需元数据 | 表前缀的列举和读取 | `02_reader_read.sql` 成功；写入被拒绝 |
| `writer` | 加载表并提交追加写入 | 列举、读取、写入表前缀；由运行时要求决定有限删除权限 | `03_writer_append.sql` 成功；不授予 DDL、DROP、广泛维护 |
| `maintainer` | 读取表、执行获批维护 Procedure | 表前缀的列举、读取、写入；只有受控维护窗口可删除 | 不等同于表 owner，不自动拥有 Namespace 管理权限 |
| `admin` | 创建 Namespace/Table、管理授权及注册恢复表 | 仅限已批准的实验前缀读写删除 | 创建 Fixture、清理、控制面恢复 |

对象存储权限必须限定到**单表或单实验前缀**，不得赋予共享 Warehouse 根目录的通配写入
或删除权限。`ListBucket` 等列举权限也应使用 prefix condition 限制。Catalog 允许读取
表名不代表对象存储可以读取 Manifest、Metadata JSON、Data File 或 Delete File；两个层
面必须同时配置。

实际身份来源由外部环境决定，例如短期令牌、实例身份、OAuth/JWT 或引擎代理凭据。
不要把每个身份的访问密钥填入 `workdir/.env` 或提交到仓库。建议为各身份使用单独、
被忽略的环境文件，并以 `ICEBERG_ENV_FILE` 选择。

## 验证顺序

1. `admin` 运行 `01_create_fixture.sql`，并记录表 Location 与当前 Snapshot。
2. 以 `reader` 身份运行 `02_reader_read.sql`，预期三条查询都成功。
3. 仍以 `reader` 身份单独运行 `04_reader_denied_write.sql.template`，预期非零退出码和
   授权错误。随后 `admin` 读取表，确认 `9399` 没有写入。
4. 以 `writer` 身份运行 `03_writer_append.sql`，预期断言返回 NULL 且 `PASS`。
5. `admin` 记录实际认证方式、Catalog/Storage 拒绝信息和结果，再运行 `05_cleanup.sql`。

如果步骤 3 成功写入，立即停止验证、记录越权事实，并用 `admin` 清理该 Fixture；不要把
该环境当作已验证的最小权限配置。应用层限制 SQL 或隐藏按钮不能替代 Catalog 和对象
存储的授权拒绝。
