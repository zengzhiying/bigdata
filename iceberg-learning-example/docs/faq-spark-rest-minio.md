# Spark/Flink + Iceberg REST Catalog + S3-Compatible Storage FAQ

本文汇总使用 Spark/Flink、Iceberg 1.11.0、REST Catalog 和 MinIO（或其他 S3-compatible storage）时，依赖加载、S3FileIO、namespace 和 Region 配置相关的常见问题。

示例版本基线：

```text
Spark:   3.5.9
Scala:   2.12
Flink:   1.20.5
Iceberg: 1.11.0
Java:    17
```

示例中的地址和凭据均为占位值。不要把真实访问密钥提交到 Git。

## Q1：`--packages` 是否要求本地已经有依赖？

`--packages` 接受 Maven 坐标，并在 Spark 启动时解析和下载依赖。例如：

```bash
spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
```

首次运行通常需要：

- 提交端能够访问 Maven Central 或配置的 Maven 仓库；
- Ivy/Maven 缓存目录可写；
- Spark 和 Scala 二进制版本与 artifact 匹配。

下载成功后，依赖会缓存到 `~/.ivy2` 或 `~/.m2`。之后缓存仍然存在时可以离线运行，但不能依赖一个不存在的缓存。

`3.5_2.12` 表示 Spark 3.5 和 Scala 2.12。Spark 3.5 不应使用 Spark 3.4 或 Scala 2.13 的 Runtime。

## Q2：离线环境如何准备依赖？

推荐在联网机器下载完整的 JAR 集合，再复制到离线机器。当前项目已经提供了 `iceberg-libs/pom.xml`，可以在联网机器执行：

```bash
cd iceberg-libs
mvn -f pom.xml dependency:copy-dependencies \
  -DoutputDirectory=. \
  -DincludeScope=runtime
```

使用 `S3FileIO` 时，POM 还必须包含同版本的 AWS bundle：

```xml
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-spark-runtime-3.5_2.12</artifactId>
  <version>1.11.0</version>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-aws-bundle</artifactId>
  <version>1.11.0</version>
</dependency>
```

`dependency:copy-dependencies` 会同时解析传递依赖。将生成的 `iceberg-libs` 目录打包并复制到离线机器：

```bash
tar czf /tmp/iceberg-libs.tar.gz -C .. iceberg-libs
# 将 /tmp/iceberg-libs.tar.gz 传到离线机器后解压到项目根目录
tar xzf /tmp/iceberg-libs.tar.gz -C /path/to/iceberg-learning-example
```

如果必须继续使用 `--packages`，也可以在联网机器上使用 `--conf spark.jars.ivy=/path/to/cache` 预热 Ivy 缓存，再把整个缓存目录复制到离线机器。不过 `--jars` 直接引用本地文件更容易验证，也不依赖离线环境的仓库解析行为。

## Q3：`--jars` 如何指定多个 JAR？

`--jars` 的值是一个参数，多个 JAR 之间使用逗号分隔：

```bash
spark-sql \
  --jars /path/a.jar,/path/b.jar,/path/c.jar
```

不要直接写 `--jars iceberg-libs/*.jar`。Shell 会把通配符展开成多个空格分隔的参数，而 Spark 要求的是一个逗号分隔的参数。

可以在当前项目中自动收集所有本地 JAR：

```bash
LIB_DIR="$(pwd)/iceberg-libs"
JARS=$(find "$LIB_DIR" -maxdepth 1 -type f -name '*.jar' | paste -sd, -)

spark-sql --jars "$JARS" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

生产或集群环境应确认这些路径能够被 Spark 分发到需要的节点；不要只在驱动节点放置一个执行端无法读取的本地路径。

## Q4：为什么出现 `S3FileIO` 找不到 `S3Exception`？

典型错误是：

```text
NoClassDefFoundError: software/amazon/awssdk/services/s3/model/S3Exception
```

真正的根因是缺少 AWS SDK v2。`org.apache.iceberg.aws.s3.S3FileIO` 使用 `software.amazon.awssdk.*`，而 Hadoop 的：

```text
aws-java-sdk-bundle-1.12.x.jar
```

是 AWS SDK v1，提供的是 `com.amazonaws.*`，不能满足 `S3FileIO`。

解决方案是添加与 Iceberg Runtime 完全相同版本的：

```text
org.apache.iceberg:iceberg-aws-bundle:1.11.0
```

外层的：

```text
Cannot find constructor for interface org.apache.iceberg.io.FileIO
```

容易误导；排查时应继续查看最底层的 `ClassNotFoundException` 或 `NoClassDefFoundError`。

可以检查 AWS SDK v2 类是否已进入 JAR：

```bash
for jar in iceberg-libs/*.jar; do
  jar tf "$jar" 2>/dev/null | \
    grep -q 'software/amazon/awssdk/services/s3/model/S3Exception.class' && \
    echo "$jar"
done
```

## Q5：添加 `iceberg-aws-bundle` 后还需要 `hadoop-aws` 和 `aws-java-sdk-bundle` 吗？

如果当前配置是：

```text
org.apache.iceberg.aws.s3.S3FileIO
```

并且使用 `s3://` Warehouse，通常只需要：

```text
iceberg-spark-runtime-3.5_2.12-1.11.0.jar
iceberg-aws-bundle-1.11.0.jar
```

`hadoop-aws` 和 AWS SDK v1 不是 `S3FileIO` 的依赖。移除不使用的 SDK v1 可以减少类路径冲突。

以下场景仍需要保留 Hadoop AWS 依赖：

- 使用 Hadoop `s3a://` 文件系统；
- 配置 `org.apache.iceberg.hadoop.HadoopFileIO`；
- Spark 或其他组件直接使用 Hadoop S3A；
- 应用代码直接依赖 `com.amazonaws.*`。

`remove_orphan_files` 默认会通过 Hadoop `FileSystem` 递归列举表 Location。若表使用 `s3://` 和 `S3FileIO`，但 Spark 未配置 Hadoop `s3` FileSystem，可能报：

```text
UnsupportedFileSystemException: No FileSystem for scheme "s3"
```

对支持 Prefix Operation 的 `S3FileIO`，可在孤儿文件 dry-run 中启用 `prefix_listing`，避免该 Hadoop 枚举路径：

```sql
CALL ice.system.remove_orphan_files(
    table => 'namespace.table',
    dry_run => true,
    prefix_listing => true
);
```

对隔离小表这是更直接的选择。大范围生产清理则应评估 Prefix Listing 的对象枚举成本，或按 Spark 的 Hadoop 版本补齐 `hadoop-aws` 与 S3A 配置，并统一使用 `s3a://` 路径。

两套依赖可以同时存在，但它们解决的是不同的访问路径，不要把 AWS SDK v1 当成 v2 的替代品。

## Q6：为什么 REST 服务记录 `404 NoSuchNamespaceException`？

执行：

```sql
CREATE NAMESPACE IF NOT EXISTS ice.learning;
```

Spark 可能先通过 REST 查询 `learning` 是否存在。namespace 不存在时，REST 服务返回：

```text
404 NoSuchNamespaceException: Namespace does not exist: learning
```

`IF NOT EXISTS` 会把这个 404 解释为“尚未存在，继续创建”。因此，如果客户端最终执行成功，并且随后：

```sql
SHOW NAMESPACES IN ice;
```

能够看到 `learning`，服务端这条 ERROR 日志通常只是检查路径产生的噪声，可以忽略。

如果客户端最终也失败，或者 namespace 创建后仍然不存在，则需要检查：

- REST Catalog 的控制面状态是否持久化；
- Spark 是否连接到了预期的 REST 服务；
- REST 服务的存储后端、权限和认证配置；
- 是否把 `ice.learning` 误解成了 namespace 名称。它实际表示 `catalog=ice`、`namespace=learning`。

## Q7：为什么插入数据时提示无法加载 Region？

典型错误是：

```text
SdkClientException: Unable to load region from any of the providers in the chain
```

Endpoint 是 MinIO 地址，并不等于 Region。AWS SDK v2 仍然需要一个区域用于请求签名。学习环境通常使用：

```text
us-east-1
```

优先在 Spark catalog 配置中显式指定：

```bash
--conf spark.sql.catalog.ice.client.region=us-east-1
```

也可以在启动 Spark 进程前设置环境变量：

```bash
export AWS_REGION=us-east-1
```

REST 服务容器中的 `AWS_REGION` 不会自动传递给宿主机上的 Spark。插入数据时，Spark 的 `S3FileIO` 是直接访问对象存储的客户端，因此 Spark 进程自身也要配置 Region。集群模式下，catalog 的 `client.region` 配置通常比只设置某台机器的环境变量更可靠。

## Q8：容器内和宿主机的 S3 Endpoint 应该如何填写？

访问同一个 MinIO 服务时，Endpoint 取决于客户端所在的位置：

| 客户端位置 | Endpoint 示例 |
| --- | --- |
| REST Catalog 容器 | `http://minio:9000` |
| 宿主机上的 Spark | `http://localhost:9000` 或宿主机 IP |
| 另一台机器上的 Spark | MinIO 对外可达的主机名或 IP |

不要在宿主机 Spark 配置中使用 Docker Compose 服务名 `minio`，除非宿主机能够解析该名称。Endpoint、凭据、Region 和 path-style 配置必须在实际发起 S3 请求的每个客户端中有效。

## Q9：一份最小可用的 Spark 配置是什么？

离线 JAR 方式的核心配置可以写成：

```bash
LIB_DIR=/opt/iceberg-learning-example/iceberg-libs
JARS=$(find "$LIB_DIR" -maxdepth 1 -type f -name '*.jar' | paste -sd, -)

spark-sql \
  --jars "$JARS" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.ice.uri=http://localhost:8181 \
  --conf spark.sql.catalog.ice.warehouse=s3://warehouse \
  --conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.ice.client.region=us-east-1 \
  --conf spark.sql.catalog.ice.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.ice.s3.path-style-access=true \
  --conf spark.sql.catalog.ice.s3.access-key-id=YOUR_ACCESS_KEY \
  --conf spark.sql.catalog.ice.s3.secret-access-key=YOUR_SECRET_KEY
```

确认 `iceberg-libs` 中只有与 Spark 3.5/Scala 2.12/Iceberg 1.11.0 匹配的运行时依赖，避免同时加载多个 Iceberg Runtime 版本。

## Q10：项目中的 `workdir/spark-sql.sh` 如何管理凭据？

项目脚本从脚本所在目录加载 `workdir/.env`，并读取以下变量：

```text
CATALOG_NAME
CATALOG_URI
CATALOG_WAREHOUSE
S3_ENDPOINT
S3_PATH_STYLE_ACCESS
AWS_REGION
MINIO_ACCESS_KEY_ID
MINIO_SECRET_ACCESS_KEY
SPARK_HOME
ICEBERG_LIB_DIR
```

可以从 [`.env.example`](../workdir/.env.example) 复制模板：

```bash
cp workdir/.env.example workdir/.env
```

将真实值写入 `workdir/.env` 后直接运行：

```bash
workdir/spark-sql.sh
```

现有 `.gitignore` 已忽略 `.env`，而 `.env.example` 不包含真实凭据，可以提交作为配置模板。脚本会在缺少 `.env`、必需配置为空、Spark 可执行文件不存在或 Runtime JAR 缺失时退出，而不会启动 Spark。完整模板见 [`workdir/.env.example`](../workdir/.env.example)。

## Q11：出现类似问题时应该按什么顺序排查？

1. 执行 `spark-sql --version`，确认 Spark 3.5.x、Scala 2.12 和 Java 17。
2. 检查 `--jars` 最终展开的字符串，确认包含 Runtime 和 `iceberg-aws-bundle`。
3. 用 `jar tf` 检查缺失类属于 AWS SDK v1 还是 v2。
4. 确认 `S3FileIO` 使用 `s3://`，而 Hadoop S3A 才使用 `hadoop-aws`。
5. 为 Spark 客户端配置 `spark.sql.catalog.<name>.client.region`。
6. 分别从 Spark、REST 容器和其他引擎测试各自可达的 Endpoint。
7. 区分 REST 的预检查 404 和真正的 SQL 失败，以客户端最终退出状态及 namespace 查询结果为准。
8. 检查 MinIO Bucket、凭据和 REST Catalog 控制面状态是否持久化。

## Q12：`RENAME COLUMN` 报 `Invalid update timestamp` 是不支持重命名吗？

不是。Iceberg 支持字段重命名，并且会保留原字段 ID。该错误表示当前提交生成的时间戳早于表 Metadata Log 中已有的最新时间戳，常见原因是提交机器时钟回拨、不同客户端时钟不一致、并发写入或 Session 持有旧表状态。

建议按以下顺序处理：

1. 停止其他写入者，重启 Spark Session 后重试。
2. 在执行提交的 Driver 主机检查 `date +%s%3N`，并与最近 Snapshot 的 `committed_at` 对照。
3. 确认 Spark Driver、Flink JobManager/TaskManager 和 REST Catalog 服务的时钟已同步。
4. 临时学习表可以执行实验的 reset 脚本后重建；不要直接编辑 Metadata JSON。

## Q13：Flink 创建 Iceberg REST Catalog 时找不到 `Configuration` 或 `HdfsConfiguration` 怎么办？

典型错误包括：

```text
ClassNotFoundException: org.apache.hadoop.conf.Configuration
ClassNotFoundException: org.apache.hadoop.hdfs.HdfsConfiguration
```

Iceberg 的 Flink Catalog 工厂会初始化 Hadoop 配置，即使实际 Warehouse 使用的是 S3 和 `S3FileIO`。精简 Flink 发行版不自带完整 Hadoop 客户端，因此只加载 `iceberg-flink-runtime` 不足以创建 Catalog。

推荐解压完整 Hadoop 客户端发行版，并在 `workdir/.env` 中设置：

```bash
HADOOP_HOME=/opt/hadoop-3.2.3
HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

`workdir/flink-sql.sh` 会执行 `hadoop classpath --glob`，并向 SQL Client 导出 `HADOOP_CLASSPATH`。不要将 Hadoop 的整套传递依赖复制到 `$FLINK_HOME/lib/`；这会与 Flink 自带依赖冲突。

## Q14：把 Hadoop 依赖复制到 Flink `lib` 后出现 `NoSuchMethodError` 怎么办？

例如：

```text
java.lang.NoSuchMethodError:
org.apache.commons.cli.Option$Builder org.apache.commons.cli.Option.builder(java.lang.String)
```

这通常表示 Hadoop 的旧传递依赖覆盖了 Flink SQL Client 自带版本。实践中常见的是 `commons-cli-1.2.jar` 覆盖了 Flink 1.20.5 所需的较新 `commons-cli` API；Jackson、Netty、Guava、SLF4J、Jetty 等也存在同类风险。

处理方式：

1. 恢复干净的 Flink 发行版 `lib`，或移除后来复制的所有非 Flink 原生 JAR。
2. 不要将 Hadoop 的全部依赖平铺到 `$FLINK_HOME/lib/`。
3. 使用完整 Hadoop 客户端和 `HADOOP_HOME`/`HADOOP_CLASSPATH` 提供 Hadoop 依赖。

## Q15：为什么 `-j iceberg-flink-runtime` 仍然找不到 `RESTCatalog`，或出现 `RESTCatalog cannot be cast to Catalog`？

两种错误分别类似：

```text
ClassNotFoundException: org.apache.iceberg.rest.RESTCatalog
ClassCastException: org.apache.iceberg.rest.RESTCatalog cannot be cast to org.apache.iceberg.catalog.Catalog
```

Flink SQL Client 的 `-j` 添加的是会话 JAR。创建 Catalog 时，SQL Gateway 需要从全局应用类加载器反射加载 `RESTCatalog`；会话类加载器对该路径不可靠。若同一 Iceberg Runtime 同时放在 `$FLINK_HOME/lib` 和 `-j` 中，还会让两个类加载器分别加载 `Catalog` 接口，从而造成无法转换的同名类。

对于本项目的 Flink SQL Client，应只在 `$FLINK_HOME/lib/` 放置以下同版本 JAR：

```text
iceberg-flink-runtime-1.20-1.11.0.jar
iceberg-aws-bundle-1.11.0.jar
```

不要放入 `iceberg-spark-runtime`，也不要在这两个 JAR 已放入 `lib` 后再用 `-j` 重复加载。`workdir/flink-sql.sh` 已遵循此规则，并在启动前检查这两个全局 JAR。

## Q16：为什么 Flink 失败日志中出现了 Catalog 凭据？

`workdir/flink-sql.sh` 会创建临时初始化 SQL，避免将凭据提交进仓库；但 Flink SQL Client 在 `CREATE CATALOG` 失败时可能把初始化 SQL 原文写入日志或命令历史。因此临时文件不等于日志层面的凭据保护。

处理方式：

1. 不要把 Flink 日志或终端输出发到公共位置。
2. 限制 `$FLINK_HOME/log` 和 SQL Client 历史文件的访问权限。
3. 若日志、终端记录或工单中已出现真实密钥，按凭据轮换策略立即废弃并重新签发。
4. 生产环境应改用外部凭据提供机制，避免在 SQL Catalog 属性中直接写入长期密钥。

## Q17：Flink SQL 能列出 Catalog 和表，但执行 SQL 时 `localhost:8081 Connection refused` 怎么办？

典型异常为：

```text
AbstractChannel$AnnotatedConnectException: Connection refused: localhost/127.0.0.1:8081
```

`sql-client.sh embedded` 可以启动交互式 SQL Client 并初始化 Catalog，但 `SELECT`、`INSERT` 及其他需要执行作业的 SQL 会提交 Flink 作业。默认配置将作业提交到 `rest.address=localhost`、端口 `8081` 的 Standalone/Session Cluster；若 JobManager 未启动，Catalog 元数据操作仍可能成功，而作业提交会失败。

在本机学习环境中，先在带有 Hadoop 类路径的 Shell 启动 Cluster：

```bash
set -a
source workdir/.env
set +a
export HADOOP_CLASSPATH="$("$HADOOP_HOME/bin/hadoop" classpath --glob)"
"$FLINK_HOME/bin/start-cluster.sh"
```

随后确认 `http://localhost:8081` 可访问，再重新进入 `workdir/flink-sql.sh`。如果使用远程 Session Cluster，应改为实际 JobManager 的 `rest.address` 和端口，并确保 JobManager、TaskManager 与 SQL Client 都能使用 Iceberg、AWS bundle 和 Hadoop 客户端类路径。

## Q18：Flink 执行 Iceberg 查询时 `Unable to load region from any of the providers` 怎么办？

典型错误为：

```text
SdkClientException: Unable to load region from any of the providers in the chain
```

错误通常在 JobManager 或 TaskManager 侧出现，表示 Iceberg `S3FileIO` 创建 AWS SDK v2 客户端时没有获得 Region。MinIO Endpoint 不等于 AWS Region；学习环境通常仍应显式使用 `us-east-1`。

在 Flink Iceberg Catalog 属性中使用 Iceberg 的通用 AWS 客户端键：

```sql
'client.region' = 'us-east-1'
```

不要使用 `s3.region`，它不是本项目当前 `S3FileIO` 配置路径使用的 Region 属性。`workdir/flink-sql.sh` 和 `flink-sql/00_catalog.sql` 已从 `AWS_REGION` 生成 `client.region`。启动 Cluster 时仍建议导出 `AWS_REGION`，但 Catalog 中的 `client.region` 才是可随作业配置传播的确定性设置。

## Q19：为什么 Flink 查询加上 `ORDER BY order_id` 后提示非时间字段不支持排序？

典型错误为：

```text
Sort on a non-time-attribute field is not supported
```

这不是 `order_id` 的数据类型不受支持。Flink 的默认流模式面向无界数据，不能等待所有未来记录后再按任意业务字段做全局排序；时间属性上的排序有不同的流式语义。

本项目现有 Flink SQL 实验读取有限表数据，应使用批模式：

```sql
SET 'execution.runtime-mode' = 'batch';
```

`workdir/flink-sql.sh` 已默认从 `FLINK_RUNTIME_MODE=batch` 设置该值，手工执行 `flink-sql/00_catalog.sql` 也会设置批模式。执行流式 CDC 实验时再将 `.env` 中的 `FLINK_RUNTIME_MODE` 改为 `streaming`，并避免对无界结果使用全局 `ORDER BY`。

## Q20：为什么 Flink 查询 Iceberg 快照表时报 `Object 'default' not found within 'ice'`？

典型错误为：

```text
SqlValidatorException: Object 'default' not found within 'ice'
```

Iceberg 元数据表名包含 `$`，需要使用反引号；但 Flink 的数据库和表名仍需作为独立标识符解析。不要将 `database.table$snapshots` 整体放进一对反引号，否则 Flink 会把它当作当前默认数据库中的单个对象，并尝试解析不存在的 `default` 数据库。

正确的 Flink SQL 形式是：

```sql
SELECT snapshot_id, committed_at, operation
FROM learning.`orders$snapshots`
ORDER BY committed_at;
```

对于 `learning_flink.orders_from_flink`，对应形式是：

```sql
FROM learning_flink.`orders_from_flink$snapshots`
```

项目中的 Flink 互操作实验已使用该语法。Trino 的元数据表引用规则不同，仍使用 `ice.learning."orders$snapshots"`。

## Q21：为什么 Flink 创建 Iceberg 表时 `PARTITIONED BY (days(event_time))` 报 SQL 解析错误？

典型错误为：

```text
ParseException: Encountered "(" ... Was expecting ")" or ","
```

Spark SQL 支持 Iceberg 隐藏分区转换：

```sql
PARTITIONED BY (days(event_time))
```

但当前 Flink SQL 的 `CREATE TABLE` 解析器在 `PARTITIONED BY` 中只接受身份分区列名。因此对本项目的 Flink 示例应改为：

```sql
PARTITIONED BY (event_time)
```

身份分区按完整时间戳分区，在真实生产表中通常粒度过细；这里仅用于最小跨引擎互操作实验。需要日期转换、隐藏分区和 Partition Spec 演进时，使用 Spark SQL 实验 02，或通过受支持的 Iceberg API 完成表定义。

跨引擎生产表通常应由 Spark 执行 DDL，再让 Flink 读取或向已有表写入。这样 Flink 不需要承担 Spark 隐藏分区转换的语法限制，也不会为了兼容 DDL 而改变表的实际分区设计。

相关配置和搭建背景见 [`rest-catalog-minio-setup.md`](rest-catalog-minio-setup.md)、[`environment-setup.md`](environment-setup.md) 和 [`iceberg-libs/README.md`](../iceberg-libs/README.md)。
