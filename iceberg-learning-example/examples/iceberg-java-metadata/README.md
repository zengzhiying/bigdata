# Iceberg Java 元数据检查器

这个工具直接通过 Iceberg REST Catalog API 读取表，不启动 Spark。它用于实验 07
观察当前 Metadata JSON、历史 Metadata JSON、当前 Snapshot 的 Manifest List 和
Manifest 文件路径。

默认表为 `learning_metadata.orders`，即实验 07 创建的专用表。可用
`METADATA_TABLE` 覆盖为其他包含 Namespace 的两段式表名。

## 构建与运行

从项目根目录使用 Java 17 构建：

```bash
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
mvn -f examples/iceberg-java-metadata/pom.xml clean package
```

首次使用或更新依赖后，先按 [`iceberg-libs/README.md`](../../iceberg-libs/README.md)
执行依赖复制命令，以确保 `iceberg-libs/slf4j-api-2.0.7.jar` 已存在。加载本地、未提交
的外部环境配置后，运行只读检查器：

```bash
set -a
source workdir/.env
set +a

java -cp "examples/iceberg-java-metadata/target/classes:iceberg-libs/iceberg-spark-runtime-3.5_2.12-1.11.0.jar:iceberg-libs/iceberg-aws-bundle-1.11.0.jar:iceberg-libs/slf4j-api-2.0.7.jar" \
  org.apache.iceberg.learning.metadata.MetadataInspectorApp --metadata-json
```

`--metadata-json` 会输出完整的当前 Metadata JSON，可重定向到本地临时文件；该输出
可能包含 Warehouse 路径，不应提交。

## 乐观并发演示

下列可选命令会创建两个独立的 REST Catalog 客户端，使它们先读取同一份 Metadata JSON。
第一个客户端新增一个 Schema 字段，第二个客户端再用旧 Schema 新增另一个字段；第二次
提交必须以 `CommitFailedException` 失败。程序捕获该预期失败并输出
`concurrent_commit_check=PASS`，同时确认成功 Schema 变更没有被旧写入覆盖：

```bash
java -cp "examples/iceberg-java-metadata/target/classes:iceberg-libs/iceberg-spark-runtime-3.5_2.12-1.11.0.jar:iceberg-libs/iceberg-aws-bundle-1.11.0.jar:iceberg-libs/slf4j-api-2.0.7.jar" \
  org.apache.iceberg.learning.metadata.MetadataInspectorApp --demonstrate-conflict
```

这是对 Catalog 原子提交条件的确定性教学演示，不是生产应用应直接调用的低层
`TableOperations` 写入模式。它会为实验表增加一个带 `concurrent_writer_a_` 前缀的
Schema 字段；完成实验后执行实验 07 的清理脚本即可删除该表。
