# 同步整条采集

旧 `legacy.capture(source,archive)` 保持原样：先 `archive.store` 后 `source.ack`，
原 `FakeSource`/`Archive` 与旧测试未改动。

新 SDK（`CONTRACT.md`）增加分片、异步确认与分区所有权转交：

- `streambridge.Pump(source, archive)`：`step()` 驱动一轮有限可用工作，返回本轮是否
  有可观察动作；`close()` 只释放本地内存，不确认也不交还。
- `archive.NewArchive`：`stage/complete/progress/pending/retire/read` 端口实现。
  分片可逐条持久暂存，只有整批（片齐、offset 覆盖连续、摘要一致）原子发布后才可查询。
- `source_sdk.SourceSDK` 是来源侧测试夹具，`Pump` 只调用
  `owned/fetch/commit/release/check/poll_events` 六个端口。

关键不变量：

1. 只有归档 `progress()` 的连续完整前沿才可用于 `commit`，绝不先确认后归档。
2. 分片/确认回复乱序、重复、丢失都不产生缺口；`check()` 与 `owned()` 是最终事实。
3. `begin_revoke` 后停止拉取，但在途 fetch 的完整交付仍会归档并结清，之后才 `release`；
   `release` 只在无在途 fetch、无未完整批次、归档前沿已被来源确认时发出。
4. 重开 `Pump` 只依赖两个端口对象：按 `owned()` 与 `progress()/pending()` 对账。
   来源 `committed` 领先归档视为丢数据，抛 `RuntimeError`。
5. 迟到的旧代次事件、旧 owner 的确认不会被应用到新 owner。

运行：

```bash
python3 -B -m unittest discover -s tests -v
python3 -B demo.py
```

`demo.py` 先跑旧 `capture`，再用 `SourceSDK`+`NewArchive` 演示分片乱序/重投、
丢失确认回复、分区转交、丢掉 Pump 重开对账以及旧代次事件迟到的完整场景。
