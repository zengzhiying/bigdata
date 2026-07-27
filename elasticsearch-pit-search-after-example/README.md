# Elasticsearch PIT 全量读取脚本

本脚本使用 Elasticsearch 8.x 的 PIT（Point in Time）和
`search_after`，遍历读取指定索引中的全部文档。每批数据会先输出批次信息，
然后每行输出一个 JSON 文档。

## 安装依赖

```bash
python3 -m pip install -r requirements.txt
```

## HTTP 使用示例

```bash
python3 pit_search_after_reader.py my-index \
  --url http://localhost:9200 \
  --username elastic \
  --password 'your-password'
```

## HTTPS 使用示例

脚本支持 HTTPS 连接。为了支持自签名证书，本示例会默认关闭证书校验，
仅建议在可信网络或开发环境中使用。

```bash
python3 pit_search_after_reader.py my-index \
  --url https://es.example.com:9200 \
  --username elastic \
  --password 'your-password' \
  --batch-size 100
```

## 参数说明

- `index`：要读取的索引或别名，必填。
- `--url`：Elasticsearch 地址，默认读取 `ELASTIC_URL`，未设置时使用
  `http://localhost:9200`。
- `--username`、`--password`：Basic Auth 凭据，也可以使用对应的环境变量
  `ELASTIC_USERNAME` 和 `ELASTIC_PASSWORD`。
- `--batch-size`：每次请求读取的文档数量，默认是 `100`。
- `--keep-alive`：PIT 保活时间，默认是 `5m`。
- `--request-timeout`：请求超时时间（秒），默认是 `120`。

## 环境变量示例

```bash
export ELASTIC_URL="https://es.example.com:9200"
export ELASTIC_USERNAME="elastic"
export ELASTIC_PASSWORD="your-password"

python3 pit_search_after_reader.py my-index
```

脚本会在遍历结束后输出总文档数；如果 Elasticsearch 返回分片错误或
PIT 已过期，脚本会报错退出，而不会静默返回不完整的数据。

## 构建二进制文件

PyInstaller 生成的二进制文件包含 Python 运行时和脚本依赖，目标机器无需
单独安装 Python。需要注意，二进制文件通常只能在与构建机器相同的操作系统
和 CPU 架构上运行；如果需要 Linux、Windows 和 macOS 版本，应分别在对应
平台构建。

安装构建依赖：

```bash
python3 -m pip install -r requirements-build.txt
```

执行构建：

```bash
./build_binary.sh
```

构建完成后，二进制文件位于：

```text
dist/es-pit-reader
```

运行方式与 Python 脚本一致：

```bash
./dist/es-pit-reader my-index \
  --url https://es.example.com:9200 \
  --username elastic \
  --password 'your-password'
```
