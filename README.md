# HiveBridge2Graph

## 项目简介

HiveBridge2Graph 是一个用于分析 Hive SQL 文件血缘关系的工具，能够：

- 递归扫描指定文件夹下的 `.sql` 和 `.hsql` 文件
- 解析 INSERT-SELECT 语句，提取表血缘关系
- 将血缘关系以图形化方式存储到 Neo4j 数据库中
- 支持配置文件驱动，灵活配置各种参数
- 提供完整的日志功能，支持不同级别的日志输出

## 功能特性

### 核心功能
1. **SQL文件扫描**: 递归扫描指定目录下的所有SQL文件
2. **智能解析**: 识别并解析INSERT-SELECT语句，忽略ADD JAR等非数据处理语句
3. **血缘提取**: 从SQL语句中提取源表和目标表的血缘关系
4. **图数据库存储**: 将血缘关系以节点和关系的形式存储到Neo4j
5. **配置驱动**: 通过配置文件管理所有参数，无硬编码
6. **日志管理**: 支持多级别日志输出，便于调试和监控

### 技术栈
- **Java 8**: 核心开发语言
- **Apache Hive**: SQL解析引擎
- **Neo4j**: 图数据库存储
- **SLF4J + Logback**: 日志框架
- **Maven**: 项目构建工具

## 快速开始

### 环境要求
- Java 8+
- Maven 3.6+
- Neo4j 4.x+ (可选，用于存储血缘关系)

### 安装步骤

1. **编译项目**
```bash
mvn clean compile
```

2. **打包项目**
```bash
mvn clean package
```

### 配置文件

编辑 `conf/config.properties` 文件：

```properties
# SQL文件夹路径
sql.folder.path=hive

# Neo4j数据库连接配置
neo4j.uri=bolt://localhost:7687
neo4j.user=neo4j
neo4j.password=password

# 日志级别 (TRACE, DEBUG, INFO, WARN, ERROR)
log.level=INFO
```

### 运行程序

**方式一：使用Maven运行**
```bash
mvn exec:java
```

**方式二：使用JAR包运行**
```bash
java -jar target/HiveBridge2Graph-1.0-SNAPSHOT.jar
```

**方式三：指定配置文件**
```bash
java -jar target/HiveBridge2Graph-1.0-SNAPSHOT.jar /path/to/custom/config.properties
```

## 项目结构

```
HiveBridge2Graph/
├── conf/
│   └── config.properties          # 配置文件
├── hive/                          # 示例SQL文件目录
├── logs/                          # 日志输出目录
├── src/main/java/com/datacyber/hive/lineage/
│   ├── Config.java               # 配置数据类
│   ├── ConfigLoader.java         # 配置加载器
│   ├── SqlFileScanner.java       # SQL文件扫描器
│   ├── HiveSqlParserUtil.java    # Hive SQL解析工具
│   ├── Neo4jWriter.java          # Neo4j写入器
│   └── SqlFileAnalysisMain.java  # 主程序入口
├── src/main/resources/
│   └── logback.xml               # 日志配置
└── pom.xml                       # Maven配置
```

## 使用示例

### 基本用法

1. 将SQL文件放置在 `hive/` 目录下
2. 配置Neo4j连接信息
3. 运行程序：

```bash
mvn exec:java
```

### 查看血缘关系

在Neo4j浏览器中执行以下Cypher查询：

```cypher
// 查看所有表节点
MATCH (t:Table) RETURN t

// 查看所有血缘关系
MATCH (source:Table)-[r:FEEDS_INTO]->(target:Table) 
RETURN source.name, target.name, r.source_file

// 查看特定表的上游依赖
MATCH (source:Table)-[r:FEEDS_INTO]->(target:Table {name: 'your_table_name'}) 
RETURN source.name, r.source_file
``` - Hive表血缘关系分析工具

一个用于提取和可视化Hive表血缘关系的工具，支持从SQL文件中解析表依赖关系并存储到Neo4j图数据库中。

## 项目功能

### 核心功能
- **SQL文件解析**: 支持解析.sql和.hsql文件，提取表血缘关系
- **表关系提取**: 自动识别SQL中的源表和目标表，构建血缘关系
- **Neo4j存储**: 将血缘关系存储到Neo4j图数据库中，便于可视化和查询
- **批量处理**: 支持递归扫描目录，批量处理大量SQL文件
- **并行处理**: 多线程并行解析，提高处理效率
- **关系去重**: 自动去除重复的表关系，确保数据准确性

### 支持的SQL类型
- Hive SQL语句
- 标准SQL语句
- 包含INSERT、CREATE TABLE AS SELECT等操作的SQL

## 项目架构

### 核心组件

1. **SQL解析器 (SqlFileParser)**
   - `CalciteSqlParser`: 基于Apache Calcite的SQL解析器
   - 支持复杂SQL语句解析，提取表依赖关系
   - 支持多种SQL方言，兼容性更好

2. **文件扫描器 (FileScanner)**
   - 递归扫描指定目录下的SQL文件
   - 支持多种文件扩展名过滤

3. **Neo4j连接器 (Neo4jConnector)**
   - 管理Neo4j数据库连接
   - 提供表关系的增删改查操作

4. **血缘分析服务 (SqlFileLineageService)**
   - 整合文件扫描和SQL解析功能
   - 提供完整的血缘分析流程

5. **配置管理器 (ConfigurationManager)**
   - 统一管理配置文件
   - 支持运行时配置读取

### 数据模型

- **TableRelation**: 表关系实体，包含源表、目标表、关系类型等信息
- **RelationType**: 关系类型枚举（如INSERT、CREATE等）

## 环境要求

- Java 8+
- Maven 3.6+
- Neo4j 4.0+

## 安装配置

### 1. Neo4j数据库安装
```bash
# 下载并启动Neo4j
wget https://neo4j.com/artifact.php?name=neo4j-community-4.4.0-unix.tar.gz
tar -xzf neo4j-community-4.4.0-unix.tar.gz
cd neo4j-community-4.4.0
bin/neo4j start
```

### 2. 配置文件设置
编辑 `src/main/resources/lineage.properties`:

```properties
# Neo4j 连接配置
lineage.neo4j.uri=bolt://localhost:7687
lineage.neo4j.username=neo4j
lineage.neo4j.password=your_password

# SQL文件扫描配置
lineage.scan.default.directory=D:\\Users\\Administrator\\Desktop\\湖南高速\\联网迁移\\hive\\
lineage.scan.file.extensions=.sql,.hsql
lineage.scan.thread.pool.size=0

# 血缘分析开关
lineage.enabled=true
```

### 3. 项目构建
```bash
mvn clean compile
mvn package
```

## 使用方法

### 方式一：使用主启动类
```bash
# 使用默认目录
java -cp target/classes com.datacyber.hive.lineage.SqlFileAnalysisMain

# 指定扫描目录
java -cp target/classes com.datacyber.hive.lineage.SqlFileAnalysisMain "/path/to/sql/files"
```

### 方式二：编程方式调用
```java
SqlFileLineageService service = new SqlFileLineageService();
SqlFileLineageService.LineageAnalysisResult result = service.analyzeDirectory("/path/to/sql/files");
service.shutdown();
```

## 输出结果

分析完成后，工具会输出以下统计信息：
- 扫描文件总数
- 成功解析文件数
- 解析失败文件数
- 提取表关系总数
- 去重后关系数
- 处理耗时



## Neo4j图模型

系统在Neo4j中创建以下图模型：

- 节点标签：`Table`，表示数据表
- 节点属性：
  - `name`：表示表名
  - `database`：表示数据库名
- 关系类型：
  - `FLOWS_TO`：表示数据从源表流向目标表
- 关系属性：
  - `queryId`：查询ID
  - `timestamp`：创建时间

## 示例查询

在Neo4j中可以使用以下Cypher查询查看表间关系：

```cypher
// 查看所有表
MATCH (t:Table) RETURN t

// 查看数据流向关系
MATCH (source:Table)-[r:FLOWS_TO]->(target:Table) RETURN source, r, target

// 查找特定表的上下游关系
MATCH (t:Table {name: 'your_table_name'})-[r]-(related) RETURN t, r, related
```

## 许可证

[Apache License 2.0](LICENSE)
