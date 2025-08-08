package com.yjj.hive.lineage;

import com.yjj.hive.lineage.model.TableRelation;
import com.yjj.hive.lineage.model.TableRelation.RelationType;
import com.yjj.hive.lineage.neo4j.Neo4jConnector;
import com.yjj.hive.lineage.util.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 主类，用于测试和演示系统功能
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOG.info("Starting HiveBridge2Graph application");
        
        // 检查配置
        ConfigurationManager configManager = ConfigurationManager.getInstance();
        LOG.info("Configuration loaded. Lineage enabled: {}", configManager.isLineageEnabled());
        
        // 测试表关系创建
        if (args.length >= 2) {
            // 如果提供了源表和目标表参数，则创建关系
            String sourceTable = args[0];
            String targetTable = args[1];
            String dbName = args.length > 2 ? args[2] : "default";
            testTableRelation(sourceTable, targetTable, dbName);
        } else {
            // 否则使用示例表关系进行测试
            testWithExampleTables();
        }
        
        LOG.info("HiveBridge2Graph application completed");
    }

    /**
     * 测试表关系创建功能
     */
    private static void testTableRelation(String sourceTable, String targetTable, String dbName) {
        LOG.info("Testing table relation creation with: {} -> {}", sourceTable, targetTable);
        
        List<TableRelation> relations = new ArrayList<>();
        String queryId = UUID.randomUUID().toString();
        
        // 创建从源表到目标表的关系
        TableRelation relation = new TableRelation(
                sourceTable,
                targetTable,
                RelationType.SOURCE_TO_TARGET,
                queryId,
                dbName
        );
        
        relations.add(relation);
        LOG.info("Created relation: {}", relation);
        
        // 保存到Neo4j
        try (Neo4jConnector connector = new Neo4jConnector()) {
            connector.saveRelations(relations);
            LOG.info("Successfully saved relation to Neo4j");
        } catch (Exception e) {
            LOG.error("Error saving to Neo4j: {}", e.getMessage(), e);
        }
    }

    /**
     * 使用示例表关系进行测试
     */
    private static void testWithExampleTables() {
        // 示例1: 单一数据流向关系
        testTableRelation("source_table1", "target_table", "default");
        
        // 示例2: 多个源表到一个目标表
        testTableRelation("source_table2", "target_table", "default");
        testTableRelation("source_table3", "target_table", "default");
        
        // 示例3: 不同数据库的表关系
        testTableRelation("db1.table1", "db2.result_table", "db1");
        testTableRelation("db1.table2", "db2.result_table", "db1");
    }
}