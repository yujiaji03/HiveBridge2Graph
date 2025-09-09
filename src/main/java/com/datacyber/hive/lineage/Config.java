package com.datacyber.hive.lineage;

/**
 * 封装应用配置
 */
public class Config {
    private final String sqlFolderPath;
    private final String neo4jUri;
    private final String neo4jUser;
    private final String neo4jPassword;
    private final String logLevel;


    // Spark 配置
    private final String sparkAppName;
    private final String sparkMaster;
    private final String sparkSqlWarehouseDir;

    // OpenLineage 配置
    private final String openLineageUrl;
    private final String openLineageNamespace;
    private final boolean openLineageEnabled;

    public Config(String sqlFolderPath, String neo4jUri, String neo4jUser, String neo4jPassword, String logLevel,
                  String sparkAppName, String sparkMaster, String sparkSqlWarehouseDir,
                  String openLineageUrl, String openLineageNamespace, boolean openLineageEnabled) {
        this.sqlFolderPath = sqlFolderPath;
        this.neo4jUri = neo4jUri;
        this.neo4jUser = neo4jUser;
        this.neo4jPassword = neo4jPassword;
        this.logLevel = logLevel;

        this.sparkAppName = sparkAppName;
        this.sparkMaster = sparkMaster;
        this.sparkSqlWarehouseDir = sparkSqlWarehouseDir;
        this.openLineageUrl = openLineageUrl;
        this.openLineageNamespace = openLineageNamespace;
        this.openLineageEnabled = openLineageEnabled;
    }

    public String getSqlFolderPath() {
        return sqlFolderPath;
    }

    public String getNeo4jUri() {
        return neo4jUri;
    }

    public String getNeo4jUser() {
        return neo4jUser;
    }

    public String getNeo4jPassword() {
        return neo4jPassword;
    }

    public String getLogLevel() {
        return logLevel;
    }


    public String getSparkAppName() {
        return sparkAppName;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public String getSparkSqlWarehouseDir() {
        return sparkSqlWarehouseDir;
    }

    public String getOpenLineageUrl() {
        return openLineageUrl;
    }

    public String getOpenLineageNamespace() {
        return openLineageNamespace;
    }

    public boolean isOpenLineageEnabled() {
        return openLineageEnabled;
    }
}
