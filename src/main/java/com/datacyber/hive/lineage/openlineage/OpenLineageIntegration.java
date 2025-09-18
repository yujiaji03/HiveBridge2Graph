package com.datacyber.hive.lineage.openlineage;

import com.datacyber.hive.lineage.Config;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpTransport;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * OpenLineage 集成类
 * 支持从 Spark Catalyst LogicalPlan 中提取血缘关系并发送到 OpenLineage 服务
 * 注意：InputLineageNode 和 OutputLineageNode 接口在不同版本中可能不同
 */
public class OpenLineageIntegration {

    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageIntegration.class);

    private final OpenLineageClient client;
    private final OpenLineage openLineage;

    /**
     * 构造函数
     *
     * @param config 配置对象
     */
    public OpenLineageIntegration(Config config) {
        this.openLineage = new OpenLineage(URI.create("https://github.com/datacyber/HiveBridge2Graph"));

        // 创建 HTTP 传输客户端
        HttpTransport transport = HttpTransport.builder()
                .uri(URI.create(config.getOpenLineageUrl()))
                .build();

        this.client = OpenLineageClient.builder()
                .transport(transport)
                .build();

        LOG.info("OpenLineage 客户端初始化完成，服务端点: {}", config.getOpenLineageUrl());
    }

    /**
     * 从 LogicalPlan 提取血缘关系并发送到 OpenLineage
     *
     * @param plan    Spark Catalyst LogicalPlan
     * @param jobName 作业名称
     * @param runId   运行ID
     */
    public void extractAndSendLineage(LogicalPlan plan, String jobName, String runId) {
        try {
            // 提取输入和输出数据集
            List<OpenLineage.InputDataset> inputDatasets = extractInputDatasets(plan);
            List<OpenLineage.OutputDataset> outputDatasets = extractOutputDatasets(plan);

            // 创建作业信息
            OpenLineage.Job job = createJob(jobName);

            // 创建运行信息
            OpenLineage.Run run = createRun(runId);

            // 发送开始事件
            sendStartEvent(job, run, inputDatasets);

            // 发送完成事件
            sendCompleteEvent(job, run, inputDatasets, outputDatasets);

            LOG.info("血缘关系已发送到 OpenLineage: 作业={}, 输入表={}, 输出表={}",
                    jobName, inputDatasets.size(), outputDatasets.size());

        } catch (Exception e) {
            LOG.error("发送血缘关系到 OpenLineage 失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 提取输入数据集
     */
    private List<OpenLineage.InputDataset> extractInputDatasets(LogicalPlan plan) {
        List<OpenLineage.InputDataset> datasets = new ArrayList<>();

        // 使用自定义的输入血缘节点提取器
        CustomInputLineageNode inputExtractor = new CustomInputLineageNode();
        List<OpenLineage.InputDataset> extractedInputs = inputExtractor.getInputs(plan);

        if (extractedInputs != null) {
            datasets.addAll(extractedInputs);
        }

        return datasets;
    }

    /**
     * 提取输出数据集
     */
    private List<OpenLineage.OutputDataset> extractOutputDatasets(LogicalPlan plan) {
        List<OpenLineage.OutputDataset> datasets = new ArrayList<>();

        // 使用自定义的输出血缘节点提取器
        CustomOutputLineageNode outputExtractor = new CustomOutputLineageNode();
        List<OpenLineage.OutputDataset> extractedOutputs = outputExtractor.getOutputs(plan);

        if (extractedOutputs != null) {
            datasets.addAll(extractedOutputs);
        }

        return datasets;
    }

    /**
     * 创建作业信息
     */
    private OpenLineage.Job createJob(String jobName) {
        return openLineage.newJobBuilder()
                .namespace("hive-bridge-2-graph")
                .name(jobName)
                .build();
    }

    /**
     * 创建运行信息
     */
    private OpenLineage.Run createRun(String runId) {
        return openLineage.newRunBuilder()
                .runId(UUID.fromString(runId))
                .build();
    }

    /**
     * 发送开始事件
     */
    private void sendStartEvent(OpenLineage.Job job, OpenLineage.Run run, List<OpenLineage.InputDataset> inputs) {
        OpenLineage.RunEvent startEvent = openLineage.newRunEventBuilder()
                .eventType(OpenLineage.RunEvent.EventType.START)
                .eventTime(ZonedDateTime.now())
                .job(job)
                .run(run)
                .inputs(inputs)
                .build();

        client.emit(startEvent);
        LOG.debug("发送开始事件: {}", startEvent);
    }

    /**
     * 发送完成事件
     */
    private void sendCompleteEvent(OpenLineage.Job job, OpenLineage.Run run,
                                   List<OpenLineage.InputDataset> inputs,
                                   List<OpenLineage.OutputDataset> outputs) {
        OpenLineage.RunEvent completeEvent = openLineage.newRunEventBuilder()
                .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                .eventTime(ZonedDateTime.now())
                .job(job)
                .run(run)
                .inputs(inputs)
                .outputs(outputs)
                .build();

        client.emit(completeEvent);
        LOG.debug("发送完成事件: {}", completeEvent);
    }

    /**
     * 从 LogicalPlan 中提取表名
     */
    private String extractTableNameFromPlan(LogicalPlan tablePlan) {
        try {
            if (tablePlan instanceof UnresolvedRelation) {
                UnresolvedRelation relation = (UnresolvedRelation) tablePlan;
                try {
                    // 尝试使用 multipartIdentifier 方法
                    return relation.tableIdentifier().toString();
                } catch (Exception e) {
                    // 如果方法不存在，返回默认值
                    LOG.debug("无法获取表名: {}", e.getMessage());
                    return "unknown_table";
                }
            }
        } catch (Exception e) {
            LOG.debug("提取表名失败: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 格式化表名
     */
    private String formatTableName(org.apache.spark.sql.catalyst.TableIdentifier tableId) {
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
     * 关闭客户端
     */
    public void close() {
        // OpenLineageClient 可能不支持 close() 方法
        // 这里只是占位符，实际实现可能需要根据具体版本调整
        LOG.info("OpenLineage 客户端关闭");
    }

    /**
     * 自定义输入血缘节点提取器
     * 实现 InputLineageNode 接口
     */
    private class CustomInputLineageNode {

        public List<OpenLineage.InputDataset> getInputs(LogicalPlan plan) {
            List<OpenLineage.InputDataset> inputs = new ArrayList<>();

            try {
                // 递归遍历 LogicalPlan 树，提取输入表
                extractInputsRecursive(plan, inputs);
            } catch (Exception e) {
                LOG.warn("提取输入数据集失败: {}", e.getMessage());
            }

            return inputs;
        }

        /**
         * 递归提取输入数据集
         */
        private void extractInputsRecursive(LogicalPlan plan, List<OpenLineage.InputDataset> inputs) {
            // 这里实现具体的输入表提取逻辑
            // 根据不同的 LogicalPlan 类型进行处理

            if (plan instanceof UnresolvedRelation) {
                UnresolvedRelation relation = (UnresolvedRelation) plan;

                try {
                    String tableName = relation.tableIdentifier().toString();
                    OpenLineage.InputDataset dataset = createInputDataset(tableName);
                    inputs.add(dataset);
                    LOG.debug("发现输入表: {}", tableName);
                } catch (Exception e) {
                    LOG.debug("无法获取输入表名: {}", e.getMessage());
                }
            }

            // 递归处理子节点
            try {
                scala.collection.Seq<LogicalPlan> children = plan.children();
                for (int i = 0; i < children.size(); i++) {
                    extractInputsRecursive(children.apply(i), inputs);
                }
            } catch (Exception e) {
                LOG.debug("处理子节点失败: {}", e.getMessage());
            }
        }

        /**
         * 创建输入数据集
         */
        private OpenLineage.InputDataset createInputDataset(String tableName) {
            return openLineage.newInputDatasetBuilder()
                    .namespace("hive")
                    .name(tableName)
                    .build();
        }
    }

    /**
     * 自定义输出血缘节点提取器
     * 实现 OutputLineageNode 接口
     */
    private class CustomOutputLineageNode {

        public List<OpenLineage.OutputDataset> getOutputs(LogicalPlan plan) {
            List<OpenLineage.OutputDataset> outputs = new ArrayList<>();

            try {
                // 递归遍历 LogicalPlan 树，提取输出表
                extractOutputsRecursive(plan, outputs);
            } catch (Exception e) {
                LOG.warn("提取输出数据集失败: {}", e.getMessage());
            }

            return outputs;
        }

        /**
         * 递归提取输出数据集
         */
        private void extractOutputsRecursive(LogicalPlan plan, List<OpenLineage.OutputDataset> outputs) {
            // 这里实现具体的输出表提取逻辑

            if (plan instanceof org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable) {
                org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable insertStmt =
                        (org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable) plan;

                String tableName = extractTableNameFromPlan(insertStmt.table());
                if (tableName != null) {
                    OpenLineage.OutputDataset dataset = createOutputDataset(tableName);
                    outputs.add(dataset);
                    LOG.debug("发现输出表: {}", tableName);
                }
            }

            // 递归处理子节点
            try {
                scala.collection.Seq<LogicalPlan> children = plan.children();
                for (int i = 0; i < children.size(); i++) {
                    extractOutputsRecursive(children.apply(i), outputs);
                }
            } catch (Exception e) {
                LOG.debug("处理子节点失败: {}", e.getMessage());
            }
        }

        /**
         * 创建输出数据集
         */
        private OpenLineage.OutputDataset createOutputDataset(String tableName) {
            return openLineage.newOutputDatasetBuilder()
                    .namespace("hive")
                    .name(tableName)
                    .build();
        }
    }
}
