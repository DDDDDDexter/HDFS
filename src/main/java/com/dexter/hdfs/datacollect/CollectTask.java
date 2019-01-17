package com.dexter.hdfs.datacollect;

import com.constant.SysConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.DateUtil;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.util.*;

@Slf4j
public class CollectTask extends TimerTask {

    @Override
    public void run() {

        /**
         * 1，基于TimerTask扫描源目录下，并获取日志文件
         * 2，将需求统计的文件移动到临时目录下
         * 3，将临时目录下的文件写入hdfs
         */
        try {
            // 读取配置文件
            Properties props = PropertyHolderLazy.getProps();

            String day = DateUtil.parseDateToStr(new Date(), DateUtil.DATE_FORMAT_YYYYMMddHH);

            File srcDir = new File(props.getProperty(SysConstant.LOG_SOURCE_DIR));

            // 读取源目录下的日志文件 根据名字前缀进行过滤
            File[] listFiles = srcDir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith(props.getProperty(SysConstant.LOG_LEGAL_PREFIX))) {
                        return true;
                    }
                    return false;
                }
            });


            log.info("探测到如下文件需要采集：" , Arrays.toString(listFiles));

            // 将目标日志写入临时目录下
            File toUploadDir = new File(props.getProperty(SysConstant.LOG_TOUPLOAD_DIR));
            for (File file : listFiles) {
                FileUtils.moveFileToDirectory(file, toUploadDir, true);
            }

            log.info("上述文件移动到了待上传目录" , toUploadDir.getAbsolutePath());

            //初始化一个hdfs客户端
            FileSystem fs = FileSystem.get(new URI(props.getProperty(SysConstant.HDFS_URI)), new Configuration(), "root");
            File[] toUploadFiles = toUploadDir.listFiles();

            // 检查HDFS中的日期目录是否存在，如果不存在，则创建 根据天命名
            Path hdfsDestPath = new Path(props.getProperty(SysConstant.HDFS_DEST_BASE_DIR) + day);
            if (!fs.exists(hdfsDestPath)) {
                fs.mkdirs(hdfsDestPath);
            }

            // 检查本地的备份目录是否存在，如果不存在，则创建 根据天命名
            File backupDir = new File(props.getProperty(SysConstant.LOG_BACKUP_BASE_DIR) + day + "/");
            if (!backupDir.exists()) {
                backupDir.mkdirs();
            }

            for (File file : toUploadFiles) {
                // 传输文件到HDFS并改名access_log_   加uuid 区分名字
                Path destPath = new Path(hdfsDestPath + "/" + UUID.randomUUID() + props.getProperty(SysConstant.HDFS_FILE_SUFFIX));
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()), destPath);

                log.info("文件写入HDFS完成：{},==>>" , file.getAbsolutePath() , destPath);

                // 将传输完成的文件移动到备份目录
                FileUtils.moveFileToDirectory(file, backupDir, true);

                log.info("文件备份完成：{},==>>{}", file.getAbsolutePath(), backupDir);

            }

        } catch (Exception e) {
           log.error("日志归档系统异常",e);
        }

    }

}
