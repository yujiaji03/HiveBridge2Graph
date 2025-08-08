package com.yjj.hive.lineage.neo4j;

import com.yjj.hive.lineage.model.TableRelation;
import com.yjj.hive.lineage.util.ConfigurationManager;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Neo4j连接器，负责将表间关系保存到Neo4j图数据库
 */
public class Neo4jConnector implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jConnector.class);
    private final Driver driver;
    private final ConfigurationManager configManager;
    private final boolean logCypher;

    public Neo4jConnector() {
        this.configManager = ConfigurationManager.getInstance();
        this.logCypher = configManager.isLogCypherEnabled();
        
        // 创建Neo4j驱动
        String uri = configManager.getNeo4jUri();
        String username = configManager.getNeo4jUsername();
        String password = configManager.getNeo4jPassword();
        
        Config config = Config.builder()
                .withConnectionTimeout(configManager.getConnectionTimeoutSeconds() * 1000, TimeUnit.MILLISECONDS)
                .build();
        
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password), config);
        
        // 初始化Neo4j，创建索引
        if (configManager.isCreateIndexesEnabled()) {
            createIndexes();
        }
    }

    /**
     * 创建Neo4j索引以提高查询性能
     */
    private void createIndexes() {
        try (Session session = driver.session()) {
            // 为表节点创建索引
            session.run("CREATE INDEX IF NOT EXISTS FOR (t:Table) ON (t.name)");
            LOG.info("Successfully created Neo4j indexes");
        } catch (Exception e) {
            LOG.error("Error creating Neo4j indexes: {}", e.getMessage(), e);
        }
    }

    /**
     * 保存表间关系到Neo4j
     *
     * @param relations 表间关系列表
     */
    public void saveRelations(List<TableRelation> relations) {
        if (relations == null || relations.isEmpty()) {
            return;
        }
        
        try (Session session = driver.session()) {
            // 使用事务批量保存关系
            session.writeTransaction(tx -> {
                for (TableRelation relation : relations) {
                    saveRelation(tx, relation);
                }
                return null;
            });
            
            LOG.info("Successfully saved {} relations to Neo4j", relations.size());
        } catch (Exception e) {
            LOG.error("Error saving relations to Neo4j: {}", e.getMessage(), e);
        }
    }

    /**
     * 保存单个表间关系到Neo4j
     */
    private void saveRelation(Transaction tx, TableRelation relation) {
        String sourceTable = relation.getSourceTable();
        String targetTable = relation.getTargetTable();
        String relationName = relation.getRelationNameForCypher();
        String queryId = relation.getQueryId();
        String databaseName = relation.getDatabaseName();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        
        // 构建Cypher查询，添加数据库名属性
        // 使用MERGE确保不会创建重复的关系
        String cypher = "MERGE (source:Table {name: $sourceTable}) "
                + "ON CREATE SET source.database = $databaseName "
                + "MERGE (target:Table {name: $targetTable}) "
                + "MERGE (source)-[r:" + relationName + "]->(target) "
                + "ON CREATE SET r.queryId = $queryId, r.timestamp = $timestamp "
                + "ON MATCH SET r.queryId = $queryId, r.timestamp = $timestamp "
                + "RETURN source, target";
        
        if (logCypher) {
            LOG.info("Executing Cypher: {}", cypher);
        }
        
        // 执行Cypher查询
        tx.run(cypher, Values.parameters(
                "sourceTable", sourceTable,
                "targetTable", targetTable,
                "queryId", queryId,
                "timestamp", timestamp,
                "databaseName", databaseName
        ));
    }

    /**
     * 清理过期的血缘关系数据
     */
    public void cleanupOldRelations() {
        int retentionDays = configManager.getRetentionDays();
        if (retentionDays <= 0) {
            // 不删除任何数据
            return;
        }
        
        try (Session session = driver.session()) {
            String cypher = "MATCH ()-[r]-() "
                    + "WHERE r.timestamp < datetime() - duration({days: $days}) "
                    + "DELETE r";
            
            if (logCypher) {
                LOG.info("Executing cleanup Cypher: {}", cypher);
            }
            
            Result result = session.run(cypher, Values.parameters("days", retentionDays));
            LOG.info("Cleaned up {} old relations", result.consume().counters().relationshipsDeleted());
        } catch (Exception e) {
            LOG.error("Error cleaning up old relations: {}", e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
    }
}
