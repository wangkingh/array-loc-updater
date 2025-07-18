# SeisHandler × ObsPy 小工具套件

> **generate_test_sac.py** & **seis_station_updater.py**  
> 作者：Wang Jingxi &amp; ChatGPT O3  
> 许可证：MIT License  

---

## 1 目标

- **generate_test_sac.py**  
  生成一套微型 SAC 文件，与台站列表 `test_stations.txt`，方便演示批量写 / 校验头段流程。  

- **seis_station_updater.py**  
  1. 使用 *SeisHandler* `SeisArray` 按 `PATTERN` 匹配 SAC；  
  2. 按台站表写入 `stla、stlo、stel`；  
  3. 产出台站散点图 `station_map.png`；  
  4. 校验写入结果，终端输出差异。  

- **sac_resp_processor.py**
  1. 使用 *SeisHandler* 和 *seisandler_resp* 匹配SAC和对应的仪器响应文件；
  2. 根据匹配结果进行取仪器响应，目前仅支持RESP格式。
---

## 2 依赖

| 库 | 版本建议 |
| --- | --- |
| Python | ≥ 3.8 |
| ObsPy | 1.3 – 1.4（已在 1.4.1 测试） |
| numpy | ≥ 1.20 |
| matplotlib | ≥ 3.3（使用 Agg 后端） |
| tqdm | ≥ 4.60 |
| SeisHandler | 本仓库自带 / 自行安装 |

安装示例：

```bash
pip install obspy numpy matplotlib tqdm
```

---

## 3 示例数据

运行

```bash
python generate_test_sac.py
```

目录结构：

```
example/
├── data/
│   └── 2024/
│       ├── XX.TST1/XX.TST1.BHZ.001.SAC
│       ├── … 共 9 个文件
└── test_stations.txt
```

台站列表示例：

```
TST1,34.5,-117.1,800
TST2,35.2,-118.0,500
TST3,36.0,-119.3,250
```

---

## 4 PATTERN 占位符

| 占位符 | 含义 |
| --- | --- |
| `{YYYY}` 年 | `{MM}` 月 | `{DD}` 日 |
| `{JJJ}` 积日 | `{HH}` 时 | `{MI}` 分 |
| `{network}` | `{station}` | `{component}` |

示例含时间字段：

```python
PATTERN = (
    "{home}/{YYYY}/{MM}{DD}/{network}.{station}/"
    "{network}.{station}.{component}.{YYYY}{MM}{DD}{HH}{MI}.SAC"
)
```

> 同一字段不可重复；若目录和文件名都要保留，可在一处用 `*` 占位。

---

## 5 运行 updater

```bash
python seis_station_updater.py
```

典型输出：

```
INFO Matched 9 files
Writing SAC headers: 100%|█████| 9/9 …
INFO station_map.png saved
INFO All headers verified OK.
```

---

## 6 常见修改

| 需求 | 修改 |
| --- | --- |
| 使用真实数据目录 | `ARRAY_DIR` & `PATTERN` |
| 不写海拔 | `ELEV_COL = None` |
| 并行线程 | `THREADS = os.cpu_count()` |
| 日志等级 | `LOG_LEVEL = "INFO" / "WARNING"` |

---

## 7 MIT License

```
MIT License

Copyright (c) 2025 Wang Jingxi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
