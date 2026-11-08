# 开发环境

项目在当前目录使用 Java 17，同时保留操作系统默认的 Java 11。推荐使用 `mise`，它只在进入本目录时应用项目工具版本。

## 使用 mise

使用操作系统适用的方式安装 `mise`，启用 Shell 集成，然后在项目根目录执行：

```bash
mise trust
mise install
mise ls
java -version
```

项目级 `.mise.toml` 选择 Temurin Java 17。`mise` 只更新当前 Shell 的 `PATH` 和 `JAVA_HOME`，不会替换或修改系统 Java 11。离开本目录后新 Shell 会恢复系统配置。

如果机器已经安装了 Java 17，可以将已有 JDK 注册到 `mise`，无需重复下载。系统 Java 11 保持不变。

## 不使用 mise

也可以只为当前 Shell 临时选择已有的 JDK 17：

```bash
export JAVA_HOME=/path/to/jdk-17
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

这只影响当前 Shell。如果希望保留 Java 11 作为系统默认版本，不要把这些导出命令加入全局 Shell 启动文件。

当前机器已确认可用的 JDK 17 路径为 `/usr/local/jdk-17.0.14+7`，可以直接执行：

```bash
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
```

## Maven 和 Spark/Flink 提交

- 启用项目级 Java 17 后再运行 Maven 和 Java 示例。
- Spark 和 Flink 任务使用外部集群支持的 Java 运行时；集群 Java 配置与本地 Shell 相互独立。
- Spark/Flink 的 Iceberg Runtime JAR 版本必须与 `config/versions.properties` 保持一致。
