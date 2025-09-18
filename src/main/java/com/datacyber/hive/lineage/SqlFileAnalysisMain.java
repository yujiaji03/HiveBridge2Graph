package com.datacyber.hive.lineage;

import com.datacyber.hive.lineage.parser.SparkCatalystParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive SQL 文件血缘分析主程序
 * 扫描指定文件夹下的SQL文件，解析INSERT-SELECT语句，提取表血缘关系并写入Neo4j
 */
public class SqlFileAnalysisMain {

    private static final Logger LOG = LoggerFactory.getLogger(SqlFileAnalysisMain.class);

    public static void main(String[] args) {
        LOG.info("=== Hive SQL 血缘分析程序启动 ===");

        try {
            // 1. 加载配置
            Config config = loadConfig(args);

            // 2. 设置日志级别
            setLogLevel(config.getLogLevel());

            // 3. 扫描SQL文件
            List<Path> sqlFiles = SqlFileScanner.scanSqlFiles(config.getSqlFolderPath());
            if (sqlFiles.isEmpty()) {
                LOG.warn("未发现任何SQL文件，程序退出");
                return;
            }

            // 4. 解析SQL文件，提取血缘关系
            List<SparkCatalystParser.TableLineage> allLineages = new ArrayList<>();

            LOG.info("使用 Spark Catalyst 解析器");
            try (SparkCatalystParser catalystParser = new SparkCatalystParser(config)) {
                LOG.info("Spark Catalyst 解析器初始化成功");

                // 使用 Spark Catalyst 解析器处理文件
                for (Path sqlFile : sqlFiles) {
                    LOG.info("正在使用 Catalyst 解析文件: {}", sqlFile);
                    List<SparkCatalystParser.TableLineage> catalystLineages = catalystParser.parseFile(sqlFile);
                    allLineages.addAll(catalystLineages);
                }
            } catch (Exception e) {
                LOG.error("Spark Catalyst 解析器执行失败: {}", e.getMessage(), e);
                throw new RuntimeException("SQL解析失败", e);
            }

            LOG.info("解析完成，共提取到 {} 条血缘关系", allLineages.size());

            if (allLineages.isEmpty()) {
                LOG.warn("未发现任何INSERT-SELECT血缘关系，程序退出");
                return;
            }

            // 5. 写入Neo4j
            try (Neo4jWriter neo4jWriter = new Neo4jWriter(config.getNeo4jUri(), config.getNeo4jUser(), config.getNeo4jPassword())) {
                // 测试连接
                boolean neo4jConnected = neo4jWriter.testConnection();
                if (!neo4jConnected) {
                    LOG.warn("Neo4j连接失败，将只进行SQL解析，不写入数据库");
                } else {
                    LOG.info("Neo4j连接成功");
                }

                // 批量写入血缘关系
                LOG.info("发现{}条血缘关系:", allLineages.size());
                for (SparkCatalystParser.TableLineage lineage : allLineages) {
                    LOG.info("  {} -> {} (来源文件: {})", lineage.getSourceTables(), lineage.getTargetTable(), lineage.getSourceFile());
                }

                if (neo4jConnected) {
                    LOG.info("开始写入{}条血缘关系到Neo4j", allLineages.size());
                    neo4jWriter.writeLineages(allLineages);
                    LOG.info("血缘关系写入完成");
                } else {
                    LOG.info("跳过Neo4j写入（连接失败）");
                }
            }

            LOG.info("=== 血缘分析程序执行完成 ===");

        } catch (Exception e) {
            LOG.error("程序执行失败", e);
            System.exit(1);
        }
    }

    /**
     * 加载配置文件
     *
     * @param args 命令行参数
     * @return 配置对象
     */
    private static Config loadConfig(String[] args) {
        try {
            if (args.length > 0) {
                // 使用命令行指定的配置文件路径
                return ConfigLoader.load(args[0]);
            } else {
                // 使用默认配置文件路径
                return ConfigLoader.load();
            }
        } catch (Exception e) {
            LOG.error("加载配置文件失败", e);
            throw new RuntimeException("配置加载失败", e);
        }
    }

    /**
     * 设置日志级别
     *
     * @param logLevel 日志级别
     */
    private static void setLogLevel(String logLevel) {
        try {
            // 设置系统属性，logback.xml会读取这个属性
            System.setProperty("log.level", logLevel.toUpperCase());
            LOG.info("日志级别设置为: {}", logLevel.toUpperCase());
        } catch (Exception e) {
            LOG.warn("设置日志级别失败: {}", e.getMessage());
        }
    }
}
