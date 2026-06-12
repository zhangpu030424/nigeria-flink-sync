# vt-pack — 独立 VT 预加载包

在任意机器上跑 `vt-preload.py`，不依赖完整 Flink 工程目录结构。

## 目录

```
/data/
  .env              # 必填（从 .env.example 复制）
  vt-preload.py
  run.sh
  py/               # 可选 Python venv
  bin/mysql         # 可选 mysql 客户端
  ddl/              # init SQL
  logs/             # --background 时自动创建
```

## 用法

```bash
cp .env.example .env   # 首次
vim .env
./run.sh --skip-count
./run.sh --background --skip-count

# 单类型快捷子命令（等价于 --vt-type <type> --skip-count）
./run.sh bank_account
./run.sh mobile
./run.sh id_number
./run.sh gaid_idfa
./run.sh all

# 后台跑银行卡
./run.sh --background bank_account
```

## 常见错误

`请先: cp .env.example .env` 但已有 `.env`：旧版 `run.sh` 误执行 `cd ..` 到上级目录。请用本包最新 `run.sh`（`cd` 留在当前目录）。
