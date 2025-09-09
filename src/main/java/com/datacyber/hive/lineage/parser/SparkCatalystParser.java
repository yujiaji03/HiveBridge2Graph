package com.datacyber.hive.lineage.parser;

import com.datacyber.hive.lineage.Config;
import com.datacyber.hive.lineage.openlineage.OpenLineageIntegration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.command.CreateViewCommand;
import org.apache.spark.sql.execution.datasources.CreateTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 基于 Spark Catalyst 和 OpenLineage 的 SQL 解析器
 * 替代正则表达式方案，提供更准确的 SQL 解析和血缘关系提取
 */
public class SparkCatalystParser implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCatalystParser.class);

    // SQL语句分割的正则表达式
    // 修复：移除 END; 作为分隔符，避免截断 CASE WHEN 表达式
    private static final Pattern SQL_DELIMITER_PATTERN = Pattern.compile(
            "(?i);\\s*(?:\\n|\\r\\n|$)",
            Pattern.MULTILINE
    );

    // Hive INSERT INTO TABLE 语法转换的正则表达式
    private static final Pattern HIVE_INSERT_INTO_TABLE_PATTERN = Pattern.compile(
            "(?i)insert\\s+into\\s+table\\s+([\\w\\.]+)\\s*\\(([^)]+)\\)\\s*(select\\s+.+)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    // 更宽松的 Hive INSERT INTO TABLE 语法匹配（支持多行和注释）
    private static final Pattern HIVE_INSERT_INTO_TABLE_LOOSE_PATTERN = Pattern.compile(
            "(?i)(insert\\s+into\\s+table\\s+[\\w\\.]+)\\s*\\(([^)]+)\\)\\s*(.*?)\\s*(select\\s+.+)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    // 普通 INSERT INTO 语法匹配（带字段列表）
    private static final Pattern HIVE_INSERT_INTO_PATTERN = Pattern.compile(
            "(?i)(insert\\s+into\\s+[\\w\\.]+)\\s*\\(([^)]+)\\)\\s*(select\\s+.+)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    // MERGE INTO 语法匹配 - 匹配完整的 MERGE INTO 语句块（从 merge 到分号）
    private static final Pattern HIVE_MERGE_INTO_PATTERN = Pattern.compile(
            "(?i)(merge\\s+into\\s+[\\w\\.]+\\s+as\\s+\\w+\\s+using\\s+[\\w\\.]+\\s+as\\s+\\w+\\s+on\\s*\\([^)]+\\)[^;]*);?",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    private final SparkSession sparkSession;
    private final Config config;
    private final OpenLineageIntegration openLineageIntegration;

    /**
     * 构造函数，初始化 Spark Session
     */
    public SparkCatalystParser() {
        this.config = null;
        this.sparkSession = createSparkSession();
        this.openLineageIntegration = null;
    }

    /**
     * 带配置的构造函数
     *
     * @param config 配置对象
     */
    public SparkCatalystParser(Config config) {
        this.config = config;
        this.sparkSession = createSparkSession();
        this.openLineageIntegration = config.isOpenLineageEnabled() ?
                new OpenLineageIntegration(config) : null;
    }

    @Override
    public void close() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }

    /**
     * 创建 Spark Session（用于 SQL 解析）
     * 使用配置文件中的 Spark 配置
     */
    private SparkSession createSparkSession() {
        try {
            LOG.info("创建 SparkSession，使用配置: appName={}, master={}, warehouse={}",
                    config.getSparkAppName(), config.getSparkMaster(), config.getSparkSqlWarehouseDir());

            return SparkSession.builder()
                    .appName(config.getSparkAppName())
                    .master(config.getSparkMaster())
                    .config("spark.sql.warehouse.dir", config.getSparkSqlWarehouseDir())
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.sql.adaptive.enabled", "false")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                    .config("spark.driver.host", "localhost")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                    .getOrCreate();
        } catch (Exception e) {
            LOG.error("创建 SparkSession 失败，可能是 Java 版本不兼容: {}", e.getMessage());
            throw new RuntimeException("SparkSession 创建失败，请确保使用 Java 11+ 或降级到兼容的 Spark 版本", e);
        }
    }

    /**
     * 解析单个SQL文件，提取表血缘关系
     *
     * @param sqlFile SQL文件路径
     * @return 表血缘关系列表
     */
    public List<TableLineage> parseFile(Path sqlFile) {
        List<TableLineage> lineages = new ArrayList<>();

        try {
            String content = new String(Files.readAllBytes(sqlFile));
            LOG.debug("开始解析文件: {}", sqlFile.getFileName());

            // 分割SQL语句
            List<String> statements = splitSqlStatements(content);
            LOG.debug("文件 {} 中发现 {} 个SQL语句", sqlFile.getFileName(), statements.size());

            for (int i = 0; i < statements.size(); i++) {
                String statement = statements.get(i).trim();
                if (statement.isEmpty()) {
                    continue;
                }

                // 检查是否为支持的SQL语句类型
                String lowerStatement = statement.toLowerCase();
                boolean isInsert = lowerStatement.contains("insert");
                boolean isCreateTable = lowerStatement.contains("create") &&
                        lowerStatement.contains("table") &&
                        lowerStatement.contains("as") &&
                        lowerStatement.contains("select");
                boolean isCreateTempTable = lowerStatement.contains("create") &&
                        lowerStatement.contains("temporary") &&
                        lowerStatement.contains("table");

                if (!isInsert && !isCreateTable && !isCreateTempTable) {
                    LOG.trace("跳过不支持的语句[{}]: {}", i + 1, statement.substring(0, Math.min(50, statement.length())));
                    continue;
                }

                TableLineage lineage = null;

                // 如果是CREATE TEMPORARY TABLE，尝试转换为CREATE TEMPORARY VIEW语法
                if (isCreateTempTable) {
                    LOG.debug("检测到CREATE TEMPORARY TABLE语句[{}]，尝试转换为CREATE TEMPORARY VIEW: {}", i + 1, statement.substring(0, Math.min(100, statement.length())));
                    TempTableConversionResult conversionResult = convertTempTableToTempViewWithOriginalName(statement);
                    if (conversionResult != null && conversionResult.convertedStatement != null) {
                        LOG.debug("成功转换为CREATE TEMPORARY VIEW语法，使用Spark Catalyst解析");
                        lineage = parseSqlStatementWithOriginalTempTableName(conversionResult.convertedStatement,
                                conversionResult.originalTableName,
                                sqlFile.toString(), i + 1);
                    } else {
                        LOG.warn("CREATE TEMPORARY TABLE语句[{}]转换失败，跳过: {}", i + 1, statement.substring(0, Math.min(100, statement.length())));
                    }
                } else {
                    // 使用Spark Catalyst解析其他语句
                    LOG.debug("使用Spark Catalyst解析语句[{}]: {}", i + 1, statement.substring(0, Math.min(100, statement.length())));
                    lineage = parseSingleStatement(statement, sqlFile.toString(), i + 1);
                }
                if (lineage != null) {
                    lineages.add(lineage);
                    LOG.info("解析到血缘关系[语句{}]: {} -> {}", i + 1, lineage.getSourceTables(), lineage.getTargetTable());
                }
            }

        } catch (IOException e) {
            LOG.error("读取文件失败: {}", sqlFile, e);
        }

        return lineages;
    }

    /**
     * 分割SQL语句
     *
     * @param content 文件内容
     * @return SQL语句列表
     */
    private List<String> splitSqlStatements(String content) {
        List<String> statements = new ArrayList<>();

        // 预处理：移除注释
        content = removeComments(content);

        // 使用正则表达式分割
        String[] parts = SQL_DELIMITER_PATTERN.split(content);

        for (String part : parts) {
            part = part.trim();
            if (!part.isEmpty()) {
                statements.add(part);
            }
        }

        // 如果没有找到分隔符，尝试简单的分号分割
        if (statements.isEmpty() && !content.trim().isEmpty()) {
            String[] simpleParts = content.split(";");
            for (String part : simpleParts) {
                part = part.trim();
                if (!part.isEmpty()) {
                    statements.add(part);
                }
            }
        }

        return statements;
    }

    /**
     * 移除SQL注释
     *
     * @param content SQL内容
     * @return 移除注释后的内容
     */
    private String removeComments(String content) {
        // 移除单行注释 --
        content = content.replaceAll("--.*?(?=\\n|\\r\\n|$)", "");
        // 移除多行注释 /* */
        content = content.replaceAll("/\\*.*?\\*/", "");
        return content;
    }

    /**
     * 预处理 Hive 语法，转换为 Spark SQL 兼容语法
     *
     * @param sql 原始 SQL 语句
     * @return 转换后的 SQL 语句
     */
    private String preprocessHiveSyntax(String sql) {
        // 首先处理变量占位符
        String processedSql = normalizeVariablePlaceholders(sql);

        // 转换 Hive 的 INSERT INTO TABLE 语法
        processedSql = convertHiveInsertIntoTable(processedSql);

        // 转换 MERGE INTO 语法
        processedSql = convertHiveMergeInto(processedSql);

        // 修复 GROUP BY 中的 CASE WHEN 表达式
        processedSql = fixGroupByCaseWhen(processedSql);

        LOG.debug("SQL预处理完成，原始长度: {}, 处理后长度: {}", sql.length(), processedSql.length());
        if (!sql.equals(processedSql)) {
            LOG.debug("检测到 Hive 语法转换");
        }

        return processedSql;
    }

    /**
     * 转换 Hive 的 INSERT INTO TABLE 语法为 Spark SQL 兼容语法
     * 将 "INSERT INTO TABLE table_name (col1, col2, ...) SELECT ..."
     * 转换为 "INSERT INTO table_name SELECT ..."
     *
     * @param sql 原始 SQL 语句
     * @return 转换后的 SQL 语句
     */
    private String convertHiveInsertIntoTable(String sql) {
        LOG.debug("开始转换 Hive INSERT INTO TABLE 语法，SQL长度: {}", sql.length());
        LOG.debug("SQL前100字符: {}", sql.substring(0, Math.min(100, sql.length())));

        // 首先尝试精确匹配
        java.util.regex.Matcher matcher = HIVE_INSERT_INTO_TABLE_PATTERN.matcher(sql);

        if (matcher.find()) {
            String tableName = matcher.group(1);
            String columnList = matcher.group(2);
            String selectClause = matcher.group(3);

            LOG.info("✅ 精确匹配转换 Hive INSERT INTO TABLE 语法: 表名={}, 列数={}",
                    tableName, columnList.split(",").length);

            // 构造 Spark SQL 兼容的语句
            return "INSERT INTO " + tableName + " " + selectClause;
        }

        // 尝试更宽松的匹配
        matcher = HIVE_INSERT_INTO_TABLE_LOOSE_PATTERN.matcher(sql);
        if (matcher.find()) {
            String insertPart = matcher.group(1); // INSERT INTO TABLE table_name
            String columnList = matcher.group(2); // 列名列表
            String middlePart = matcher.group(3);  // 中间部分（可能包含注释）
            String selectPart = matcher.group(4);  // SELECT 部分

            // 提取表名
            String tableName = insertPart.replaceAll("(?i)insert\\s+into\\s+table\\s+", "").trim();

            LOG.info("✅ 宽松匹配转换 Hive INSERT INTO TABLE 语法: 表名={}, 列数={}",
                    tableName, columnList.split(",").length);
            LOG.debug("转换前SQL: {}", sql.substring(0, Math.min(200, sql.length())));

            // 构造 Spark SQL 兼容的语句，只保留 SELECT 部分
            String result = "INSERT INTO " + tableName + " " + selectPart;
            LOG.debug("转换后SQL: {}", result.substring(0, Math.min(200, result.length())));
            return result;
        }

        // 尝试匹配普通 INSERT INTO 语法（带字段列表）
        matcher = HIVE_INSERT_INTO_PATTERN.matcher(sql);
        if (matcher.find()) {
            String insertPart = matcher.group(1); // INSERT INTO table_name
            String columnList = matcher.group(2); // 列名列表
            String selectPart = matcher.group(3);  // SELECT 部分

            // 提取表名
            String tableName = insertPart.replaceAll("(?i)insert\\s+into\\s+", "").trim();

            LOG.info("✅ 转换 INSERT INTO 语法（移除字段列表）: 表名={}, 列数={}",
                    tableName, columnList.split(",").length);
            LOG.debug("转换前SQL: {}", sql.substring(0, Math.min(200, sql.length())));

            // 构造 Spark SQL 兼容的语句，移除字段列表
            String result = "INSERT INTO " + tableName + " " + selectPart;
            LOG.debug("转换后SQL: {}", result.substring(0, Math.min(200, result.length())));
            return result;
        }

        LOG.debug("未匹配到需要转换的 INSERT 语法，返回原始SQL");
        return sql;
    }

    /**
     * 转换 Hive 的 MERGE INTO 语法为 Spark SQL 兼容语法
     * 由于 Spark SQL 不直接支持 MERGE INTO，将其转换为注释形式以便提取血缘关系
     *
     * @param sql 原始 SQL 语句
     * @return 转换后的 SQL 语句
     */
    private String convertHiveMergeInto(String sql) {
        LOG.debug("开始转换 MERGE INTO 语法，SQL长度: {}", sql.length());

        java.util.regex.Matcher matcher = HIVE_MERGE_INTO_PATTERN.matcher(sql);
        String processedSql = sql;

        while (matcher.find()) {
            LOG.info("✅ 检测到 MERGE INTO 语句，转换为注释形式以便血缘分析");

            // 提取目标表和源表信息
            String mergeStatement = matcher.group(1);

            // 使用正则表达式提取表名
            java.util.regex.Pattern targetTablePattern = Pattern.compile(
                    "(?i)merge\\s+into\\s+([\\w\\.]+)\\s+as\\s+(\\w+)",
                    Pattern.CASE_INSENSITIVE
            );
            java.util.regex.Pattern sourceTablePattern = Pattern.compile(
                    "(?i)using\\s+([\\w\\.]+)\\s+as\\s+(\\w+)",
                    Pattern.CASE_INSENSITIVE
            );

            java.util.regex.Matcher targetMatcher = targetTablePattern.matcher(mergeStatement);
            java.util.regex.Matcher sourceMatcher = sourceTablePattern.matcher(mergeStatement);

            String targetTable = "unknown_target";
            String sourceTable = "unknown_source";

            if (targetMatcher.find()) {
                targetTable = targetMatcher.group(1);
            }
            if (sourceMatcher.find()) {
                sourceTable = sourceMatcher.group(1);
            }

            LOG.info("MERGE INTO 血缘关系: {} -> {}", sourceTable, targetTable);

            // 转换为简化的 INSERT INTO 语句以便 Catalyst 解析血缘关系
            String convertedStatement = "-- MERGE INTO converted to INSERT for lineage analysis\n" +
                    "INSERT INTO " + targetTable + " SELECT * FROM " + sourceTable;

            // 替换原始的 MERGE INTO 语句
            processedSql = processedSql.replace(matcher.group(0), convertedStatement);

            LOG.debug("MERGE INTO 转换结果: {}", convertedStatement.substring(0, Math.min(200, convertedStatement.length())));
        }

        if (!sql.equals(processedSql)) {
            LOG.debug("MERGE INTO 语法转换完成");
        } else {
            LOG.debug("未匹配到 MERGE INTO 语法，返回原始SQL");
        }
        return processedSql;
    }

    /**
     * 修复 GROUP BY 中的 CASE WHEN 表达式
     * 将 GROUP BY 中的 CASE WHEN 表达式替换为对应的 SELECT 列别名
     *
     * @param sql 原始 SQL 语句
     * @return 修复后的 SQL 语句
     */
    private String fixGroupByCaseWhen(String sql) {
        LOG.debug("开始删除 GROUP BY 中的 CASE WHEN 表达式，SQL长度: {}", sql.length());

        // 查找 GROUP BY 子句
        // 修改正则表达式，确保能够正确截取到分号前的所有内容
        Pattern groupByPattern = Pattern.compile(
                "(?i)(.*?)(group\\s+by\\s+)(.*?)(?=\\s*;)",
                Pattern.DOTALL | Pattern.CASE_INSENSITIVE
        );

        java.util.regex.Matcher groupByMatcher = groupByPattern.matcher(sql);
        if (!groupByMatcher.find()) {
            LOG.debug("未找到 GROUP BY 子句，返回原始SQL");
            return sql;
        }

        String beforeGroupBy = groupByMatcher.group(1);
        String groupByKeyword = groupByMatcher.group(2);
        String groupByClause = groupByMatcher.group(3);
        String afterGroupBy = sql.substring(groupByMatcher.end());

        LOG.info("原始 GROUP BY 子句: {}", groupByClause);

        String fixedGroupByClause = groupByClause;
        int removedCount = 0;

        // 使用循环逐个删除CASE WHEN表达式，确保删除所有的
        Pattern caseWhenPattern = Pattern.compile(
                "(?i)\\bcase\\s+when\\b.*\\bend\\b",
                Pattern.DOTALL | Pattern.CASE_INSENSITIVE
        );

        boolean foundAny = true;
        while (foundAny) {
            java.util.regex.Matcher matcher = caseWhenPattern.matcher(fixedGroupByClause);
            foundAny = false;

            if (matcher.find()) {
                String caseExpression = matcher.group();
                LOG.info("✅ 删除 GROUP BY 中的 CASE WHEN 表达式: {}",
                        caseExpression.trim().length() > 50 ? caseExpression.trim().substring(0, 50) + "..." : caseExpression.trim());

                // 删除找到的CASE WHEN表达式
                fixedGroupByClause = matcher.replaceFirst("");
                removedCount++;
                foundAny = true;
            }
        }

        if (removedCount > 0) {
            // 清理多余的逗号和空白
            fixedGroupByClause = fixedGroupByClause
                    .replaceAll("\\s*,\\s*,+\\s*", ", ")  // 多个连续逗号变成一个
                    .replaceAll("^\\s*,+\\s*", "")         // 开头的逗号
                    .replaceAll("\\s*,+\\s*$", "")         // 结尾的逗号
                    .replaceAll("\\s+", " ")             // 多个空格变成一个
                    .trim();

            // 进一步清理可能残留的逗号问题
            while (fixedGroupByClause.contains(",, ")) {
                fixedGroupByClause = fixedGroupByClause.replace(",, ", ", ").replace(",, ", ",");
            }

            // 清理开头和结尾的逗号
            if (fixedGroupByClause.startsWith(",")) {
                fixedGroupByClause = fixedGroupByClause.substring(1).trim();
            }
            if (fixedGroupByClause.endsWith(",")) {
                fixedGroupByClause = fixedGroupByClause.substring(0, fixedGroupByClause.length() - 1).trim();
            }

            // 如果 GROUP BY 子句为空，则删除整个 GROUP BY
            if (fixedGroupByClause.trim().isEmpty()) {
                LOG.info("GROUP BY 子句已为空，删除整个 GROUP BY");
                return beforeGroupBy.trim() + " " + afterGroupBy;
            }

            LOG.debug("修复后 GROUP BY 子句: {}", fixedGroupByClause);
            LOG.info("✅ 修复 GROUP BY 中的 CASE WHEN 表达式，删除了 {} 个表达式", removedCount);

            return beforeGroupBy + groupByKeyword + fixedGroupByClause + afterGroupBy;
        } else {
            LOG.debug("未检测到需要删除的 GROUP BY CASE WHEN 语法，返回原始SQL");
            return sql;
        }
    }

    /**
     * 解析单个SQL语句
     */
    private TableLineage parseSingleStatement(String sql, String fileName, int statementIndex) {
        try {
            // 检查是否是临时表语句，如果是则特殊处理
            if (sql.toLowerCase().contains("create temporary table")) {
                TempTableConversionResult conversionResult = convertTempTableToTempViewWithOriginalName(sql);
                if (conversionResult != null) {
                    // 使用转换后的语句解析，但保留原始表名用于血缘关系
                    return parseSqlStatementWithOriginalTempTableName(
                            conversionResult.convertedStatement,
                            conversionResult.originalTableName,
                            fileName,
                            statementIndex
                    );
                }
            }

            // 预处理 Hive 语法，转换为 Spark SQL 兼容语法
            String processedSql = preprocessHiveSyntax(sql);

            // 使用 Catalyst 解析器解析 SQL
            LogicalPlan plan = sparkSession.sessionState().sqlParser().parsePlan(processedSql);

            // 提取血缘关系
            return extractLineageFromPlan(plan, fileName, statementIndex);

        } catch (Exception e) {
            LOG.warn("Catalyst解析SQL语句[{}]失败: {}, 错误: {}", statementIndex,
                    sql.substring(0, Math.min(100, sql.length())), e.getMessage());
        }

        return null;
    }

    /**
     * 解析单个SQL语句，使用 Spark Catalyst
     *
     * @param statement      SQL语句
     * @param fileName       文件名（用于日志）
     * @param statementIndex 语句索引
     * @return 表血缘关系，如果解析失败则返回null
     */
    private TableLineage parseSqlStatement(String statement, String fileName, int statementIndex) {
        try {
            // 使用 Spark Catalyst 解析 SQL
            LogicalPlan logicalPlan = sparkSession.sessionState().sqlParser().parsePlan(statement);

            // 提取血缘关系
            TableLineage lineage = extractLineageFromPlan(logicalPlan, fileName, statementIndex);

            if (lineage != null) {
                return lineage;
            }

        } catch (Exception e) {
            LOG.warn("Catalyst解析SQL语句[{}]失败: {}, 错误: {}", statementIndex,
                    statement.substring(0, Math.min(100, statement.length())), e.getMessage());
        }

        return null;
    }

    /**
     * 解析带有原始临时表名信息的SQL语句
     *
     * @param statement         转换后的SQL语句
     * @param originalTableName 原始的完整表名（包含数据库前缀）
     * @param fileName          文件名
     * @param statementIndex    语句索引
     * @return 表血缘关系
     */
    private TableLineage parseSqlStatementWithOriginalTempTableName(String statement, String originalTableName, String fileName, int statementIndex) {
        try {
            LOG.debug("解析临时表SQL语句[{}]: {}", statementIndex, statement.substring(0, Math.min(100, statement.length())));
            LogicalPlan plan = sparkSession.sessionState().sqlParser().parsePlan(statement);
            TableLineage lineage = extractLineageFromPlan(plan, fileName, statementIndex);

            // 如果解析成功且有原始表名，则替换目标表名为原始的完整表名
            if (lineage != null && originalTableName != null) {
                LOG.debug("使用原始表名 {} 替换解析得到的表名 {}", originalTableName, lineage.getTargetTable());
                return new TableLineage(originalTableName, lineage.getSourceTables(), lineage.getSourceFile());
            }

            return lineage;
        } catch (Exception e) {
            LOG.warn("Spark Catalyst解析临时表失败[语句{}]: {}", statementIndex, e.getMessage());
            return null;
        }
    }

    /**
     * 将 CREATE TEMPORARY TABLE 语法转换为 CREATE TEMPORARY VIEW 语法
     *
     * @param statement 原始SQL语句
     * @return 转换后的SQL语句，如果转换失败则返回null
     */

    private String convertTempTableToTempView(String statement) {
        TempTableConversionResult result = convertTempTableToTempViewWithOriginalName(statement);
        return result != null ? result.convertedStatement : null;
    }

    private TempTableConversionResult convertTempTableToTempViewWithOriginalName(String statement) {
        try {
            LOG.debug("原始语句: {}", statement.substring(0, Math.min(200, statement.length())));

            // 使用正则表达式将 CREATE TEMPORARY TABLE 替换为 CREATE TEMPORARY VIEW
            String converted = statement.replaceAll(
                    "(?i)CREATE\\s+TEMPORARY\\s+TABLE",
                    "CREATE TEMPORARY VIEW"
            );

            // 提取原始的完整表名（包含数据库前缀和变量占位符）
            String originalTableName = null;
            java.util.regex.Pattern tableNamePattern = java.util.regex.Pattern.compile(
                    "(?i)CREATE\\s+TEMPORARY\\s+VIEW\\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_${}]*)?)",
                    java.util.regex.Pattern.CASE_INSENSITIVE
            );
            java.util.regex.Matcher matcher = tableNamePattern.matcher(converted);
            if (matcher.find()) {
                originalTableName = matcher.group(1);
                // 对原始表名应用变量占位符规范化
                originalTableName = normalizeVariablePlaceholders(originalTableName);
            }

            // 移除临时视图名称中的数据库前缀，因为 Spark 不允许临时视图带数据库前缀
            // 匹配模式：CREATE TEMPORARY VIEW database.table_name AS（支持变量占位符）
            converted = converted.replaceAll(
                    "(?i)(CREATE\\s+TEMPORARY\\s+VIEW\\s+)([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_${}]*)(\\s+AS)",
                    "$1$3$4"
            );

            // 对转换后的语句应用变量占位符规范化
            converted = normalizeVariablePlaceholders(converted);

            // 验证转换是否成功
            if (!converted.equals(statement)) {
                LOG.debug("转换后语句: {}", converted.substring(0, Math.min(200, converted.length())));
                LOG.info("✅ 成功转换 CREATE TEMPORARY TABLE 为 CREATE TEMPORARY VIEW，原始表名: {}", originalTableName);
                return new TempTableConversionResult(converted, originalTableName);
            } else {
                LOG.debug("❌ 语句未发生转换，可能不包含 CREATE TEMPORARY TABLE 语法");
            }
        } catch (Exception e) {
            LOG.warn("转换CREATE TEMPORARY TABLE语法失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 从 LogicalPlan 中提取血缘关系
     *
     * @param plan           Spark Catalyst LogicalPlan
     * @param fileName       文件名
     * @param statementIndex 语句索引
     * @return 表血缘关系
     */
    private TableLineage extractLineageFromPlan(LogicalPlan plan, String fileName, int statementIndex) {
        try {
            // 查找 InsertIntoTable (Spark 2.4.x)
            if (plan instanceof InsertIntoTable) {
                return extractFromInsertIntoTable((InsertIntoTable) plan, fileName, statementIndex);
            }
            // 查找 CreateDataSourceTableAsSelectCommand (CREATE TABLE AS SELECT)
            else if (plan instanceof CreateDataSourceTableAsSelectCommand) {
                return extractFromCreateTableAsSelect((CreateDataSourceTableAsSelectCommand) plan, fileName, statementIndex);
            }
            // 查找 CreateTable (CREATE TABLE AS SELECT 的另一种形式)
            else if (plan instanceof CreateTable) {
                return extractFromCreateTable((CreateTable) plan, fileName, statementIndex);
            }
            // 查找 CreateViewCommand (CREATE TEMPORARY VIEW)
            else if (plan instanceof CreateViewCommand) {
                return extractFromCreateViewCommand((CreateViewCommand) plan, fileName, statementIndex);
            } else {
                // 递归查找子计划
                try {
                    scala.collection.Seq<LogicalPlan> children = plan.children();
                    // 使用 JavaConverters 将 Scala 集合转换为 Java 集合
                    java.util.List<LogicalPlan> childrenList = JavaConverters.seqAsJavaListConverter(children).asJava();
                    for (LogicalPlan child : childrenList) {
                        TableLineage lineage = extractLineageFromPlan(child, fileName, statementIndex);
                        if (lineage != null) {
                            return lineage;
                        }
                    }
                } catch (Exception e) {
                    LOG.debug("处理子计划失败: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.debug("提取血缘关系失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 使用HiveSqlParserUtil回退解析CREATE TEMPORARY TABLE语句
     * @param statement SQL语句
     * @param fileName 文件名
     * @param statementIndex 语句索引
     * @return 表血缘关系，如果解析失败则返回null
     */

    /**
     * 从 InsertIntoTable 中提取血缘关系 (Spark 2.4.x)
     */
    private TableLineage extractFromInsertIntoTable(InsertIntoTable insertStmt, String fileName, int statementIndex) {
        try {
            // 获取目标表
            String targetTable = extractTableName(insertStmt.table());

            // 获取源表
            Set<String> sourceTables = extractSourceTables(insertStmt.query());

            if (targetTable != null && !sourceTables.isEmpty()) {
                String sourceFileInfo = fileName + "#" + statementIndex;
                return new TableLineage(targetTable, sourceTables, sourceFileInfo);
            }
        } catch (Exception e) {
            LOG.debug("从InsertIntoTable提取血缘关系失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 从 CreateDataSourceTableAsSelectCommand 中提取血缘关系
     */
    private TableLineage extractFromCreateTableAsSelect(CreateDataSourceTableAsSelectCommand createStmt, String fileName, int statementIndex) {
        try {
            // 获取目标表名并规范化
            String targetTable = normalizeTableName(createStmt.table().identifier().toString());

            // 获取源表
            Set<String> sourceTables = extractSourceTables(createStmt.query());

            if (targetTable != null && !sourceTables.isEmpty()) {
                String sourceFileInfo = fileName + "#" + statementIndex;
                return new TableLineage(targetTable, sourceTables, sourceFileInfo);
            }
        } catch (Exception e) {
            LOG.debug("从CreateDataSourceTableAsSelectCommand提取血缘关系失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 从 CreateTable 中提取血缘关系
     */
    private TableLineage extractFromCreateTable(CreateTable createStmt, String fileName, int statementIndex) {
        try {
            // 获取目标表名并规范化
            String targetTable = normalizeTableName(createStmt.tableDesc().identifier().toString());

            // 检查是否有查询部分（AS SELECT）
            if (createStmt.query().isDefined()) {
                LogicalPlan queryPlan = createStmt.query().get();
                Set<String> sourceTables = extractSourceTables(queryPlan);

                if (targetTable != null && !sourceTables.isEmpty()) {
                    String sourceFileInfo = fileName + "#" + statementIndex;
                    return new TableLineage(targetTable, sourceTables, sourceFileInfo);
                }
            }
        } catch (Exception e) {
            LOG.debug("从CreateTable提取血缘关系失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 从 CreateViewCommand 中提取血缘关系
     */
    private TableLineage extractFromCreateViewCommand(CreateViewCommand createViewCmd, String fileName, int statementIndex) {
        try {
            // 获取视图名称
            String viewName = normalizeTableName(createViewCmd.name().toString());
            LOG.debug("提取CreateViewCommand血缘关系，视图名: {}", viewName);

            // 获取查询计划
            LogicalPlan queryPlan = createViewCmd.child();
            Set<String> sourceTables = extractSourceTables(queryPlan);

            if (viewName != null && !sourceTables.isEmpty()) {
                String sourceFileInfo = fileName + "#" + statementIndex;
                LOG.debug("成功提取CreateViewCommand血缘关系: {} -> {}", sourceTables, viewName);
                return new TableLineage(viewName, sourceTables, sourceFileInfo);
            } else {
                LOG.debug("CreateViewCommand血缘关系提取失败: viewName={}, sourceTables={}", viewName, sourceTables);
            }
        } catch (Exception e) {
            LOG.debug("从CreateViewCommand提取血缘关系失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 标准化表名，移除反引号标识符
     */
    private String normalizeTableName(String tableName) {
        if (tableName == null) {
            return null;
        }
        // 移除反引号
        String normalized = tableName.replace("`", "");

        // 处理变量占位符，将其规范化为通用格式
        normalized = normalizeVariablePlaceholders(normalized);

        return normalized;
    }

    // 注意：InsertIntoDataSourceCommand 在某些Spark版本中可能不存在
    // 如果需要支持该功能，请根据具体的Spark版本添加相应的实现

    /**
     * 规范化变量占位符，将动态表名转换为通用格式以便血缘关系分析
     *
     * @param tableName 包含变量占位符的表名
     * @return 规范化后的表名
     */
    private String normalizeVariablePlaceholders(String tableName) {
        if (tableName == null) {
            return null;
        }

        String normalized = tableName;

        // 时间相关变量占位符规范化
        // ${YYYYMMDD} -> _YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{YYYYMMDD\\}", "_YYYYMMDD_");
        // ${N0D_YYYYMMDD} -> _N0D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{N0D_YYYYMMDD\\}", "_N0D_YYYYMMDD_");
        // ${L1D_YYYYMMDD} -> _L1D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{L1D_YYYYMMDD\\}", "_L1D_YYYYMMDD_");
        // ${N1D_YYYYMMDD} -> _N1D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{N1D_YYYYMMDD\\}", "_N1D_YYYYMMDD_");
        // ${L3D_YYYYMMDD} -> _L3D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{L3D_YYYYMMDD\\}", "_L3D_YYYYMMDD_");
        // ${N5D_YYYYMMDD} -> _N5D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{N5D_YYYYMMDD\\}", "_N5D_YYYYMMDD_");
        // ${L7D_YYYYMMDD} -> _L7D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{L7D_YYYYMMDD\\}", "_L7D_YYYYMMDD_");
        // ${N7D_YYYYMMDD} -> _N7D_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{N7D_YYYYMMDD\\}", "_N7D_YYYYMMDD_");
        // ${L2M_YYYYMMDD} -> _L2M_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{L2M_YYYYMMDD\\}", "_L2M_YYYYMMDD_");
        // ${N1M_YYYYMMDD} -> _N1M_YYYYMMDD_
        normalized = normalized.replaceAll("\\$\\{N1M_YYYYMMDD\\}", "_N1M_YYYYMMDD_");
        // ${YYYYMMDDHH} -> _YYYYMMDDHH_
        normalized = normalized.replaceAll("\\$\\{YYYYMMDDHH\\}", "_YYYYMMDDHH_");
        // ${HH} -> _HH_
        normalized = normalized.replaceAll("\\$\\{HH\\}", "_HH_");
        // ${L1H_HH} -> _L1H_HH_
        normalized = normalized.replaceAll("\\$\\{L1H_HH\\}", "_L1H_HH_");

        // 数据库相关变量占位符规范化
        // ${DB_TMP} -> tmp (使用更合理的数据库名)
        normalized = normalized.replaceAll("\\$\\{DB_TMP\\}", "tmp");
        // ${DB_ODS} -> ngods
        normalized = normalized.replaceAll("\\$\\{DB_ODS\\}", "ngods");
        // ${DB_DWD} -> ngdwd
        normalized = normalized.replaceAll("\\$\\{DB_DWD\\}", "ngdwd");
        // ${DB_DWS} -> ngdws
        normalized = normalized.replaceAll("\\$\\{DB_DWS\\}", "ngdws");
        // ${DB_STG} -> ngstg2
        normalized = normalized.replaceAll("\\$\\{DB_STG\\}", "ngstg2");
        // ${DB_DM} -> ngdwt
        normalized = normalized.replaceAll("\\$\\{DB_DM\\}", "ngdwt");

        // 业务相关变量占位符规范化
        // ${CENTER_NO} -> _CENTER_NO_
        normalized = normalized.replaceAll("\\$\\{CENTER_NO\\}", "_CENTER_NO_");
        // ${CURRENT_DATE} -> _CURRENT_DATE_
        normalized = normalized.replaceAll("\\$\\{CURRENT_DATE\\}", "_CURRENT_DATE_");
        // ${CURRENT_TIME} -> _CURRENT_TIME_
        normalized = normalized.replaceAll("\\$\\{CURRENT_TIME\\}", "_CURRENT_TIME_");

        // 其他日期格式变量
        // ${YYYY} -> _YYYY_
        normalized = normalized.replaceAll("\\$\\{YYYY\\}", "_YYYY_");
        // ${MM} -> _MM_
        normalized = normalized.replaceAll("\\$\\{MM\\}", "_MM_");
        // ${DD} -> _DD_
        normalized = normalized.replaceAll("\\$\\{DD\\}", "_DD_");
        // ${N0D_YYYY_MM_DD} -> _N0D_YYYY_MM_DD_
        normalized = normalized.replaceAll("\\$\\{N0D_YYYY_MM_DD\\}", "_N0D_YYYY_MM_DD_");
        // ${N1D_YYYY} -> _N1D_YYYY_
        normalized = normalized.replaceAll("\\$\\{N1D_YYYY\\}", "_N1D_YYYY_");
        // ${N1D_MM} -> _N1D_MM_
        normalized = normalized.replaceAll("\\$\\{N1D_MM\\}", "_N1D_MM_");
        // ${N1D_DD} -> _N1D_DD_
        normalized = normalized.replaceAll("\\$\\{N1D_DD\\}", "_N1D_DD_");
        // ${L1D_YYYY} -> _L1D_YYYY_
        normalized = normalized.replaceAll("\\$\\{L1D_YYYY\\}", "_L1D_YYYY_");
        // ${L1D_MM} -> _L1D_MM_
        normalized = normalized.replaceAll("\\$\\{L1D_MM\\}", "_L1D_MM_");
        // ${L1D_DD} -> _L1D_DD_
        normalized = normalized.replaceAll("\\$\\{L1D_DD\\}", "_L1D_DD_");

        LOG.debug("变量占位符规范化: {} -> {}", tableName, normalized);

        return normalized;
    }

    /**
     * 提取表名
     */
    private String extractTableName(LogicalPlan tablePlan) {
        try {
            if (tablePlan instanceof UnresolvedRelation) {
                UnresolvedRelation relation = (UnresolvedRelation) tablePlan;
                // 获取表名，根据 Spark 版本可能需要调整
                try {
                    String tableName = relation.tableIdentifier().toString();
                    return normalizeTableName(tableName);
                } catch (Exception e) {
                    // 如果 tableIdentifier 方法调用失败，尝试其他方法
                    LOG.debug("无法获取表名: {}", e.getMessage());
                    return "unknown_table";
                }
            } else if (tablePlan instanceof SubqueryAlias) {
                SubqueryAlias alias = (SubqueryAlias) tablePlan;
                return extractTableName(alias.child());
            }
        } catch (Exception e) {
            LOG.debug("提取表名失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 提取源表
     */
    private Set<String> extractSourceTables(LogicalPlan queryPlan) {
        Set<String> sourceTables = new HashSet<>();
        extractSourceTablesRecursive(queryPlan, sourceTables);
        return sourceTables;
    }

    /**
     * 递归提取源表
     */
    private void extractSourceTablesRecursive(LogicalPlan plan, Set<String> sourceTables) {
        try {
            if (plan instanceof UnresolvedRelation) {
                UnresolvedRelation relation = (UnresolvedRelation) plan;
                try {
                    // 使用 tableIdentifier 方法
                    String tableName = relation.tableIdentifier().toString();
                    sourceTables.add(normalizeTableName(tableName));
                } catch (Exception e) {
                    // 如果方法调用失败，使用默认值
                    LOG.debug("无法获取表名: {}", e.getMessage());
                    sourceTables.add("unknown_table");
                }
            } else if (plan instanceof Join) {
                Join join = (Join) plan;
                extractSourceTablesRecursive(join.left(), sourceTables);
                extractSourceTablesRecursive(join.right(), sourceTables);
            } else if (plan instanceof SubqueryAlias) {
                SubqueryAlias alias = (SubqueryAlias) plan;
                extractSourceTablesRecursive(alias.child(), sourceTables);
            } else {
                // 递归处理子计划
                try {
                    scala.collection.Seq<LogicalPlan> children = plan.children();
                    // 使用 JavaConverters 将 Scala 集合转换为 Java 集合
                    java.util.List<LogicalPlan> childrenList = JavaConverters.seqAsJavaListConverter(children).asJava();
                    for (LogicalPlan child : childrenList) {
                        extractSourceTablesRecursive(child, sourceTables);
                    }
                } catch (Exception e) {
                    LOG.debug("处理子节点失败: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.debug("递归提取源表失败: {}", e.getMessage());
        }
    }

    /**
     * 格式化表名
     */
    private String formatTableName(TableIdentifier tableId) {
        try {
            if (tableId.database().isDefined()) {
                return tableId.database().get() + "." + tableId.table();
            } else {
                return tableId.table();
            }
        } catch (Exception e) {
            LOG.debug("格式化表名失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 临时表转换结果类
     */
    private static class TempTableConversionResult {
        final String convertedStatement;
        final String originalTableName;

        TempTableConversionResult(String convertedStatement, String originalTableName) {
            this.convertedStatement = convertedStatement;
            this.originalTableName = originalTableName;
        }
    }

    /**
     * 表血缘关系数据类
     */
    public static class TableLineage {
        private final String targetTable;
        private final Set<String> sourceTables;
        private final String sourceFile;

        public TableLineage(String targetTable, Set<String> sourceTables, String sourceFile) {
            this.targetTable = targetTable;
            this.sourceTables = sourceTables;
            this.sourceFile = sourceFile;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public Set<String> getSourceTables() {
            return sourceTables;
        }

        public String getSourceFile() {
            return sourceFile;
        }

        @Override
        public String toString() {
            return String.format("TableLineage{target='%s', sources=%s, file='%s'}",
                    targetTable, sourceTables, sourceFile);
        }
    }
}
