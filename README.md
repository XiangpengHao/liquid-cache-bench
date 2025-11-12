A collection of datasets and queries, primarily used to test & benchmark [LiquidCache](https://github.com/XiangpengHao/LiquidCache).

Criteria for things in the repo:
1. Real world data only, no synthetic data (i.e., no TPC-H, TPC-DS, etc.). Synthetic queries are allowed, but must have real world semantics. 
2. Data should be in Parquet format. No pre-processing allowed; query engines should directly process parquet data; caching is allowed, but the query engine should report both cold and warm results.
3. Each benchmark should include both data and queries, along with the expected results (i.e., results should be deterministic).
4. All the setup scripts should run with a single [`uv`](https://docs.astral.sh/uv/getting-started/installation/) command. No other explicit dependencies.

We welcome contributions!
