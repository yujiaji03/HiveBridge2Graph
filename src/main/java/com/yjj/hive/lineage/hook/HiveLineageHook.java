package com.yjj.hive.lineage.hook;

import com.yjj.hive.lineage.model.TableRelation;
import com.yjj.hive.lineage.model.TableRelation.RelationType;
import com.yjj.hive.lineage.neo4j.Neo4jConnector;
import com.yjj.hive.lineage.util.ConfigurationManager;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Hive Hook实现类，用于拦截Hive查询并提取表间关系
 * 直接从HookContext获取输入和输出表信息，不再使用SQL解析器
 */
public class HiveLineageHook implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageHook.class);
    private final Neo4jConnector neo4jConnector;
    private final boolean enabled;

    public HiveLineageHook() {
        ConfigurationManager configManager = ConfigurationManager.getInstance();
        this.enabled = configManager.isLineageEnabled();
        this.neo4jConnector = createNeo4jConnector();
        LOG.info("HiveLineageHook initialized. Enabled: {}", enabled);
    }
    
    /**
     * 创建Neo4jConnector实例，可在测试中被覆盖以提供mock对象
     * 
     * @return Neo4jConnector实例
     */
    protected Neo4jConnector createNeo4jConnector() {
        return new Neo4jConnector();
    }

    @Override
    public void run(HookContext hookContext) {
        if (!enabled) {
            LOG.debug("Lineage tracking is disabled. Skipping hook execution.");
            return;
        }

        try {
            // 获取查询计划和SQL语句
            QueryPlan plan = hookContext.getQueryPlan();
            String query = plan.getQueryStr();
            String queryId = UUID.randomUUID().toString();
            
            LOG.info("Processing query: {}", query);
            
            // 从HookContext获取输入和输出表信息
            Set<ReadEntity> inputs = hookContext.getInputs();
            Set<WriteEntity> outputs = hookContext.getOutputs();
            
            // 提取表间关系
            List<TableRelation> relations = extractTableRelations(inputs, outputs, queryId);
            
            if (relations.isEmpty()) {
                LOG.info("No table relations found in the query.");
                return;
            }
            
            // 将关系保存到Neo4j
            neo4jConnector.saveRelations(relations);
            
            LOG.info("Successfully processed query and saved {} relations to Neo4j", relations.size());
        } catch (Exception e) {
            LOG.error("Error processing query in HiveLineageHook", e);
        }
    }
    
    /**
     * 从输入和输出表信息中提取表间关系
     * 
     * @param inputs 输入表实体集合
     * @param outputs 输出表实体集合
     * @param queryId 查询ID
     * @return 表间关系列表
     */
    private List<TableRelation> extractTableRelations(Set<ReadEntity> inputs, Set<WriteEntity> outputs, String queryId) {
        List<TableRelation> relations = new ArrayList<>();
        
        // 过滤出表类型的输入实体
        List<Table> inputTables = new ArrayList<>();
        for (ReadEntity input : inputs) {
            if (input.getType() == Entity.Type.TABLE) {
                inputTables.add(input.getTable());
            }
        }
        
        // 过滤出表类型的输出实体
        List<Table> outputTables = new ArrayList<>();
        for (WriteEntity output : outputs) {
            if (output.getType() == Entity.Type.TABLE) {
                outputTables.add(output.getTable());
            }
        }
        
        LOG.info("Found {} input tables and {} output tables", inputTables.size(), outputTables.size());
        
        // 为每个输出表创建与每个输入表的关系（数据流向）
        for (Table outputTable : outputTables) {
            String targetTableName = outputTable.getTableName();
            String targetDbName = outputTable.getDbName();
            
            for (Table inputTable : inputTables) {
                String sourceTableName = inputTable.getTableName();
                String sourceDbName = inputTable.getDbName();
                
                // 创建从源表到目标表的关系
                TableRelation relation = new TableRelation(
                        sourceTableName,
                        targetTableName,
                        RelationType.SOURCE_TO_TARGET,
                        queryId,
                        sourceDbName
                );
                
                relations.add(relation);
                LOG.debug("Created relation: {} -> {}", sourceTableName, targetTableName);
            }
        }
        
        return relations;
    }
}