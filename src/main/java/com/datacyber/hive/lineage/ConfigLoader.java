package com.datacyber.hive.lineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 读取 conf/config.properties 配置文件并构造 Config 对象
 */
public class ConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

    private static final String DEFAULT_CONF_PATH = "conf/config.properties";

    public static Config load() {
        return load(DEFAULT_CONF_PATH);
    }

    public static Config load(String confPath) {
        Path path = Paths.get(confPath);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("配置文件不存在: " + path.toAbsolutePath());
        }
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(path.toFile())) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("读取配置文件失败", e);
        }

        // 基础配置
        String sqlFolder = props.getProperty("sql.folder.path");
        String neo4jUri = props.getProperty("neo4j.uri");
        String neo4jUser = props.getProperty("neo4j.user");
        String neo4jPassword = props.getProperty("neo4j.password");
        String logLevel = props.getProperty("log.level", "INFO");


        // Spark 配置
        String sparkAppName = props.getProperty("spark.app.name", "HiveBridge2Graph-SQLParser");
        String sparkMaster = props.getProperty("spark.master", "local[1]");
        String sparkSqlWarehouseDir = props.getProperty("spark.sql.warehouse.dir", "/tmp/spark-warehouse");

        // OpenLineage 配置
        String openLineageUrl = props.getProperty("openlineage.url", "http://localhost:5000");
        String openLineageNamespace = props.getProperty("openlineage.namespace", "hive-bridge-2-graph");
        boolean openLineageEnabled = Boolean.parseBoolean(props.getProperty("openlineage.enabled", "false"));

        if (sqlFolder == null || neo4jUri == null || neo4jUser == null || neo4jPassword == null) {
            throw new IllegalStateException("配置文件缺少必要字段");
        }

        LOG.info("成功加载配置: sqlFolder={}, neo4jUri={}, user={}, logLevel={}, openLineageEnabled={}",
                sqlFolder, neo4jUri, neo4jUser, logLevel, openLineageEnabled);

        return new Config(sqlFolder, neo4jUri, neo4jUser, neo4jPassword, logLevel,
                sparkAppName, sparkMaster, sparkSqlWarehouseDir,
                openLineageUrl, openLineageNamespace, openLineageEnabled);
    }
}
