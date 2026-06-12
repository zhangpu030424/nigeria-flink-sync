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
```

## 常见错误

`请先: cp .env.example .env` 但已有 `.env`：旧版 `run.sh` 误执行 `cd ..` 到上级目录。请用本包最新 `run.sh`（`cd` 留在当前目录）。
