package com.datacyber.hive.lineage;

import com.datacyber.hive.lineage.parser.SparkCatalystParser;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Neo4j 数据库写入工具类
 */
public class Neo4jWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Neo4jWriter.class);

    private final Driver driver;

    public Neo4jWriter(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        LOG.info("Neo4j连接已建立: {}", uri);
    }

    /**
     * 写入表血缘关系到Neo4j
     *
     * @param lineage 表血缘关系
     */
    public void writeLineage(SparkCatalystParser.TableLineage lineage) {
        try (Session session = driver.session()) {
            // 创建目标表节点
            createTableNode(session, lineage.getTargetTable());

            // 创建源表节点并建立关系
            for (String sourceTable : lineage.getSourceTables()) {
                createTableNode(session, sourceTable);
                createLineageRelationship(session, sourceTable, lineage.getTargetTable(), lineage.getSourceFile());
            }

            LOG.debug("成功写入血缘关系: {} -> {}", lineage.getSourceTables(), lineage.getTargetTable());
        } catch (Exception e) {
            LOG.error("写入Neo4j失败: {}", lineage, e);
        }
    }

    /**
     * 创建表节点
     *
     * @param session   Neo4j会话
     * @param tableName 表名
     */
    private void createTableNode(Session session, String tableName) {
        String cypher = "MERGE (t:Table {name: $tableName}) " +
                "ON CREATE SET t.created = datetime(), t.type = 'hive_table' " +
                "ON MATCH SET t.updated = datetime()";

        Map<String, Object> params = new HashMap<>();
        params.put("tableName", tableName);

        session.run(cypher, params);
        LOG.trace("创建/更新表节点: {}", tableName);
    }

    /**
     * 创建血缘关系
     *
     * @param session     Neo4j会话
     * @param sourceTable 源表
     * @param targetTable 目标表
     * @param sourceFile  源文件
     */
    private void createLineageRelationship(Session session, String sourceTable, String targetTable, String sourceFile) {
        String cypher = "MATCH (source:Table {name: $sourceTable}), (target:Table {name: $targetTable}) " +
                "MERGE (source)-[r:FEEDS_INTO]->(target) " +
                "ON CREATE SET r.created = datetime(), r.source_file = $sourceFile, r.relationship_type = 'data_lineage' " +
                "ON MATCH SET r.updated = datetime(), r.source_file = $sourceFile";

        Map<String, Object> params = new HashMap<>();
        params.put("sourceTable", sourceTable);
        params.put("targetTable", targetTable);
        params.put("sourceFile", sourceFile);

        session.run(cypher, params);
        LOG.trace("创建/更新血缘关系: {} -> {}", sourceTable, targetTable);
    }

    /**
     * 批量写入血缘关系
     *
     * @param lineages 血缘关系列表
     */
    public void writeLineages(java.util.List<SparkCatalystParser.TableLineage> lineages) {
        LOG.info("开始批量写入 {} 条血缘关系", lineages.size());

        int successCount = 0;
        int failCount = 0;

        for (SparkCatalystParser.TableLineage lineage : lineages) {
            try {
                writeLineage(lineage);
                successCount++;
            } catch (Exception e) {
                failCount++;
                LOG.error("写入血缘关系失败: {}", lineage, e);
            }
        }

        LOG.info("批量写入完成，成功: {}, 失败: {}", successCount, failCount);
    }

    /**
     * 测试Neo4j连接
     *
     * @return 连接是否成功
     */
    public boolean testConnection() {
        try (Session session = driver.session()) {
            Result result = session.run("RETURN 1 as test");
            if (result.hasNext()) {
                LOG.info("Neo4j连接测试成功");
                return true;
            }
        } catch (Exception e) {
            LOG.error("Neo4j连接测试失败", e);
        }
        return false;
    }

    /**
     * 清空所有表血缘数据（谨慎使用）
     */
    public void clearAllLineageData() {
        try (Session session = driver.session()) {
            session.run("MATCH (t:Table)-[r:FEEDS_INTO]-() DELETE r");
            session.run("MATCH (t:Table) DELETE t");
            LOG.warn("已清空所有表血缘数据");
        } catch (Exception e) {
            LOG.error("清空数据失败", e);
        }
    }

    @Override
    public void close() {
        if (driver != null) {
            driver.close();
            LOG.info("Neo4j连接已关闭");
        }
    }
}
