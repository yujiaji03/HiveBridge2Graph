package com.datacyber.hive.lineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * 递归扫描指定文件夹下的 .sql 和 .hsql 文件
 */
public class SqlFileScanner {

    private static final Logger LOG = LoggerFactory.getLogger(SqlFileScanner.class);

    /**
     * 扫描指定目录下的所有 SQL 文件
     *
     * @param folderPathStr 文件夹路径，多个路径用逗号分隔
     * @return SQL 文件路径列表
     */
    public static List<Path> scanSqlFiles(String folderPathStr) {
        List<Path> sqlFiles = new ArrayList<>();
        String[] folderPaths = folderPathStr.split(",");

        for (String folderPath : folderPaths) {
            folderPath = folderPath.trim();
            Path startPath = Paths.get(folderPath);

            if (!Files.exists(startPath)) {
                LOG.warn("指定的文件夹不存在: {}", startPath.toAbsolutePath());
                continue;
            }

            if (!Files.isDirectory(startPath)) {
                LOG.warn("指定的路径不是文件夹: {}", startPath.toAbsolutePath());
                continue;
            }

            LOG.info("开始扫描文件夹: {}", startPath.toAbsolutePath());

            try {
                Files.walkFileTree(startPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        String fileName = file.getFileName().toString().toLowerCase();
                        if (fileName.endsWith(".sql") || fileName.endsWith(".hsql")) {
                            sqlFiles.add(file);
                            LOG.debug("发现SQL文件: {}", file.toAbsolutePath());
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        LOG.warn("访问文件失败: {}, 错误: {}", file, exc.getMessage());
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                LOG.error("扫描文件夹时发生错误: {}", e.getMessage(), e);
            }
        }

        LOG.info("扫描完成，共发现 {} 个SQL文件", sqlFiles.size());
        return sqlFiles;
    }
}
