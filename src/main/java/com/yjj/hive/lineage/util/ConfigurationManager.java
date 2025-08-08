package com.yjj.hive.lineage.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理类，负责加载和管理系统配置
 */
public class ConfigurationManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationManager.class);
    private static final String CONFIG_FILE = "lineage.properties";
    private static ConfigurationManager instance;
    private final Properties properties;

    private ConfigurationManager() {
        properties = new Properties();
        loadProperties();
    }

    public static synchronized ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                LOG.error("Unable to find {} in classpath", CONFIG_FILE);
                return;
            }
            properties.load(input);
            LOG.info("Successfully loaded configuration from {}", CONFIG_FILE);
        } catch (IOException e) {
            LOG.error("Error loading configuration file: {}", e.getMessage(), e);
        }
    }

    public boolean isLineageEnabled() {
        return Boolean.parseBoolean(properties.getProperty("lineage.enabled", "true"));
    }

    public boolean isHookEnabled() {
        return Boolean.parseBoolean(properties.getProperty("lineage.hook.enabled", "true"));
    }

    public String getNeo4jUri() {
        return properties.getProperty("lineage.neo4j.uri", "bolt://localhost:7687");
    }

    public String getNeo4jUsername() {
        return properties.getProperty("lineage.neo4j.username", "neo4j");
    }

    public String getNeo4jPassword() {
        return properties.getProperty("lineage.neo4j.password", "neo4j");
    }

    public int getConnectionTimeoutSeconds() {
        return Integer.parseInt(properties.getProperty("lineage.connection.timeout.seconds", "30"));
    }

    public int getMaxRetryAttempts() {
        return Integer.parseInt(properties.getProperty("lineage.max.retry.attempts", "3"));
    }

    public boolean isLogCypherEnabled() {
        return Boolean.parseBoolean(properties.getProperty("lineage.log.cypher", "false"));
    }

    public boolean isIgnoreTempTables() {
        return Boolean.parseBoolean(properties.getProperty("lineage.ignore.temp.tables", "true"));
    }

    public String[] getTempTablePrefixes() {
        String prefixes = properties.getProperty("lineage.temp.table.prefixes", "tmp_,temp_");
        return prefixes.split(",");
    }

    public boolean isCreateIndexesEnabled() {
        return Boolean.parseBoolean(properties.getProperty("lineage.neo4j.create.indexes", "true"));
    }

    public int getRetentionDays() {
        return Integer.parseInt(properties.getProperty("lineage.retention.days", "0"));
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}