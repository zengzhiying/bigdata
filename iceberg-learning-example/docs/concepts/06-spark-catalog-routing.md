# 06 - Spark 如何选择 Iceberg Catalog

## 先说结论

Iceberg Runtime JAR 只提供 Spark 插件实现和 Iceberg API，它本身不知道应该连接哪个 Catalog。

真正决定写入目标的是三部分：

1. `spark.sql.catalog.<名称>` 注册了一个 Spark Catalog 插件。
2. `spark.sql.catalog.<名称>.*` 提供该 Catalog 的具体配置，例如 URI。
3. SQL 中的三段式表名使用了哪个 Catalog 名称。

## 一个完整配置

例如下面的配置注册了名为 `ice` 的 Catalog：

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.ice.uri=https://catalog.example.com
spark.sql.catalog.ice.warehouse=s3://warehouse
```

配置的含义是：

| 配置 | 含义 |
| --- | --- |
| `spark.sql.catalog.ice` | 注册名为 `ice` 的 Spark Catalog 插件。 |
| `org.apache.iceberg.spark.SparkCatalog` | Spark 侧的 Catalog 适配器。 |
| `catalog-impl` | 适配器内部使用的 Iceberg Catalog 实现。 |
| `uri` | REST Catalog 服务地址。 |
| `warehouse` | 表数据和元数据使用的 Warehouse，具体是否由服务端接管取决于 Catalog 实现。 |

Runtime JAR 中包含 `SparkCatalog`、`RESTCatalog` 等实现类，但不会预先绑定 `catalog.example.com` 或任何 Warehouse。

## 表名如何路由

Iceberg Spark SQL 使用三段式名称：

```text
catalog.namespace.table
```

例如：

```sql
CREATE TABLE ice.learning.orders (...)
USING iceberg;

INSERT INTO ice.learning.orders VALUES (...);

SELECT * FROM ice.learning.orders;
```

这里的 `ice` 会被 Spark SQL 解析器识别为 Catalog 注册名，然后将后续操作交给刚才注册的 Catalog 插件。插件再通过配置的 URI 访问 REST Catalog。

因此，当前项目实验中使用：

```text
ice.learning.orders
```

就是在明确告诉 Spark：使用名为 `ice` 的 Catalog，而不是其他 Catalog。

## 不写 Catalog 名称会怎样

如果使用两段式名称：

```sql
SELECT * FROM learning.orders;
```

Spark 会使用当前 Catalog。默认情况下通常是 `spark_catalog`，它不一定指向 Iceberg REST Catalog。

因此跨 Catalog 项目中建议始终使用三段式名称，或者显式执行：

```sql
USE CATALOG ice;
SELECT * FROM learning.orders;
```

为了避免实验受到当前 Session 状态影响，本项目 SQL 默认使用完整的 `ice.learning.orders` 名称。

## 配置多个 Catalog

同一个 Spark Session 可以注册多个 Catalog：

```text
spark.sql.catalog.ice_dev=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice_dev.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.ice_dev.uri=https://dev-catalog.example.com

spark.sql.catalog.ice_prod=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice_prod.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.ice_prod.uri=https://prod-catalog.example.com
```

此时可以明确区分目标：

```sql
SELECT * FROM ice_dev.learning.orders;
SELECT * FROM ice_prod.learning.orders;
```

这也是避免误写生产 Catalog 的重要手段。

## Runtime、Catalog 和 Warehouse 的分工

```text
Spark SQL
   │  解析 ice.learning.orders
   ▼
SparkCatalog 插件
   │  读取 spark.sql.catalog.ice.*
   ▼
RESTCatalog 客户端
   │  请求 https://catalog.example.com
   ▼
Catalog 服务
   │  返回表元数据位置和提交结果
   ▼
S3 / Warehouse 中的数据文件与元数据文件
```

- Runtime JAR：提供代码。
- Spark Catalog 配置：决定使用哪个插件和哪个服务地址。
- Catalog 服务：管理表名、表元数据位置和提交。
- Warehouse：保存数据文件、删除文件、Manifest 和 Metadata JSON。

## 如何验证实际写入目标

不要只根据配置文件判断，可以用以下方式验证：

1. 执行 `SHOW NAMESPACES IN ice`，确认请求确实通过 `ice` Catalog。
2. 创建一张带唯一名称的测试表。
3. 在 Catalog 服务或其管理界面检查表注册信息。
4. 查询表的 `snapshots` 和 `files` 元数据表。
5. 检查 Warehouse 中是否生成了对应的 Metadata JSON 和数据文件。

如果表名没有写 `ice`，或者 `USE CATALOG` 指向了其他名称，就可能写入另一个 Catalog。
