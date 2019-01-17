package com.dexter.hdfs.datacollect;

import com.constant.SysConstant;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

public class BackupCleanTask extends TimerTask {

    @Override
    public void run() {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        long now = new Date().getTime();
        try {
            // 检测本地目录
            File backupBaseDir = new File("D:/logs/backup/tmp");
            File[] dayBackDir = backupBaseDir.listFiles();

            // 判断备份记录是否超过24小时
            for (File dir : dayBackDir) {
                long time = sdf.parse(dir.getName()).getTime();
                if (now - time > SysConstant.oneDay) {
                    FileUtils.deleteDirectory(dir);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
