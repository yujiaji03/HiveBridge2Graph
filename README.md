# HiveBridge2Graph

一个用于提取Hive查询中表间血缘关系并将其转换为Neo4j图的工具。

## 功能特点

- 通过实现Hive的`ExecuteWithHookContext`接口拦截查询
- 直接从HookContext获取输入和输出表信息
- 识别表之间的数据流向关系
- 将表作为图的节点，表间关系作为边保存到Neo4j图数据库
- 可配置的保留期限，自动清理过期数据

## 系统架构

系统主要由以下几个组件组成：

1. **HiveLineageHook**: 实现Hive的`ExecuteWithHookContext`接口，拦截查询并从HookContext获取表信息
2. **TableRelation**: 表示表之间关系的模型类
3. **Neo4jConnector**: 负责连接Neo4j并保存表间关系
4. **ConfigurationManager**: 管理系统配置

## 安装与配置

### 前提条件

- Java 8+
- Apache Hive 3.x
- Neo4j 4.x

### 编译

```bash
mvn clean package
```

编译后的JAR文件将位于`target`目录下。

### 配置

在`src/main/resources/lineage.properties`中配置系统参数：

```properties
# Neo4j 连接配置
lineage.neo4j.uri=bolt://localhost:7687
lineage.neo4j.username=neo4j
lineage.neo4j.password=neo4j

# 血缘分析开关
lineage.enabled=true

# 是否忽略临时表
lineage.ignore.temp.tables=true

# 临时表前缀
lineage.temp.table.prefixes=tmp_,temp_
```

### 在Hive中配置Hook

在Hive的`hive-site.xml`中添加以下配置：

```xml
<property>
  <name>hive.exec.post.hooks</name>
  <value>com.yjj.hive.lineage.hook.HiveLineageHook</value>
</property>
```

确保HiveBridge2Graph的JAR文件在Hive的classpath中。

## 使用方法

### 作为Hive Hook使用

配置好Hive Hook后，系统会自动拦截所有Hive查询，提取表间关系并保存到Neo4j。

### 独立使用

也可以通过Main类直接测试表关系创建功能：

```bash
java -cp target/HiveBridge2Graph-1.0-SNAPSHOT.jar com.yjj.hive.lineage.Main source_table target_table default
```

参数说明：
1. 源表名
2. 目标表名
3. 数据库名（可选，默认为"default"）

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