# PyIceberg 元数据 API 示例

`inspect_table.py` 使用 PyIceberg `0.9.1` 直接连接 REST Catalog，加载并打印一个已存在 Iceberg 表的 Schema、当前位置和当前 Snapshot。它刻意只读取元数据，因此可以作为 Spark 或 Flink 写入后的轻量 API 检查入口。

先加载 `workdir/.env`，再创建隔离的 Python 环境并安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r examples/pyiceberg/requirements.txt
```

默认读取 PySpark 示例创建的 `learning_api.orders_pyspark`。先运行该示例，或通过 `PYICEBERG_TABLE` 选择一个已经存在的三段式表名：

```bash
python examples/pyiceberg/inspect_table.py
PYICEBERG_TABLE=learning_flink.orders_from_flink python examples/pyiceberg/inspect_table.py
```

PyIceberg 示例不创建或删除表，因此不需要清理。该示例已在项目当前外部环境手动验证；引擎、Catalog、存储或版本变更后，应重新执行。
