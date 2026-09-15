OSS 批量备份 — Windows 包
==========================

本目录包含在 Windows 上跑备份所需的全部脚本（无需 Git Bash、无需整仓同步）。

包含文件
--------
  oss_backup_from_csv.py   主程序
  oss_backup.env.example   配置模板
  run_list.bat             预览将备份哪些桶（--list）
  run_backup.bat           开始下载
  buckets_20260914.csv     控制台导出的 Bucket 列表

不要从 Mac 拷贝 oss_backup.env（含密钥）；在 Windows 上本地创建。

前置依赖
--------
1. Python 3.8+  https://www.python.org/downloads/
   安装时勾选 “Add python.exe to PATH”

2. 阿里云 ossutil64
   https://help.aliyun.com/document_detail/120075.html
   解压 ossutil64.exe，加入 PATH，或在 oss_backup.env 里设置 OSSUTIL_PATH

3. 磁盘空间约 2TB+（见 run_list.bat 输出的 total_GiB）

配置
----
  copy oss_backup.env.example oss_backup.env
  编辑 oss_backup.env：
    OSS_BACKUP_ROOT=备份盘符路径，如 D:/OSS-backup
    BUCKETS_CSV=本目录 CSV 的完整路径，如 D:/tools/oss-backup-windows/buckets_20260914.csv
    OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET（或用 ossutil config 配置后留空）

运行
----
  双击 run_list.bat   确认 planned 桶与容量
  双击 run_backup.bat  长期运行，可挂任务计划程序

命令行：
  cd /d D:\path\to\oss-backup-windows
  python oss_backup_from_csv.py --list
  python oss_backup_from_csv.py

说明
----
  默认跳过 tanzania*、th*、归档/冷归档桶（SKIP_ARCHIVE=1）。
  印度备份桶 loan-core-data-india-sg-bak、india-filess-new-bak 需 ossutil restore 解冻后，
  在 oss_backup.env 设 SKIP_ARCHIVE=0 或 ONLY_BUCKETS=桶名 再单独备份。
  本地目录结构：{OSS_BACKUP_ROOT}/{地域}/{Bucket名}/... 与 OSS 对象 key 一致。
