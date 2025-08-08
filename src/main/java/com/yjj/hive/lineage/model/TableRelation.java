package com.yjj.hive.lineage.model;

import java.util.Objects;

/**
 * 表示表之间的关系模型
 */
public class TableRelation {
    // 关系类型枚举
    public enum RelationType {
        SOURCE_TO_TARGET("FLOWS_TO");       // 数据从源表流向目标表
        
        private final String relationName;
        
        RelationType(String relationName) {
            this.relationName = relationName;
        }
        
        public String getRelationName() {
            return relationName;
        }
    }
    
    private String sourceTable;      // 源表名
    private String targetTable;      // 目标表名
    private RelationType relationType; // 关系类型
    private String queryId;          // 查询ID，用于关联同一查询中的多个关系
    private String databaseName;     // 数据库名称
    
    public TableRelation() {
    }
    
    public TableRelation(String sourceTable, String targetTable, RelationType relationType) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.relationType = relationType;
    }
    
    public TableRelation(String sourceTable, String targetTable, RelationType relationType, String queryId, String databaseName) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.relationType = relationType;
        this.queryId = queryId;
        this.databaseName = databaseName;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public RelationType getRelationType() {
        return relationType;
    }

    public void setRelationType(RelationType relationType) {
        this.relationType = relationType;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
    
    /**
     * 获取用于Neo4j Cypher查询的关系名称
     */
    public String getRelationNameForCypher() {
        return relationType.getRelationName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableRelation that = (TableRelation) o;
        return Objects.equals(sourceTable, that.sourceTable) &&
                Objects.equals(targetTable, that.targetTable) &&
                relationType == that.relationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, targetTable, relationType);
    }

    @Override
    public String toString() {
        return "TableRelation{" +
                "sourceTable='" + sourceTable + '\'' +
                ", targetTable='" + targetTable + '\'' +
                ", relationType=" + relationType +
                ", queryId='" + queryId + '\'' +
                ", databaseName='" + databaseName + '\'' +
                '}';
    }
}