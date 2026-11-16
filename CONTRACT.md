# 分片采集至归档和来源所有权转交

Python3.11标准库。旧legacy.capture(source,archive)调用source.take()取得
{partition,offset,body}直到None，archive.store(partition,offset,body)成功后source.ack(partition,offset)。
原FakeSource及Archive和旧测试保持；新SDK端口与旧接口不同，本轮不改变旧capture语义。

新增streambridge.Pump(source,archive)，step()执行一轮可用工作并返回bool（本轮有可观察动作则True）。
close()仅释放本地资源，不自行确认或交还。可丢弃Pump并用同一两个端口对象重新构造，端口内部的
来源和归档持久状态保持，而Pump内存全丢。无真正线程或网络；所有异步回包通过poll_events可重现。
source及archive都是公开端口，Pump不得访问其私有属性、SQLite表或通过反射偷看未来事件。

source.owned() -> [{partition,owner,committed}]：当前已分配给本采集端的分区及所有权token，
committed为该所有权下已经被来源接纳的最大确认offset（初始-1）；token字符串同分区不复用。
source.fetch(partition,owner) -> request字符串，发出拉取；每分区最多一个未完整返回拉取请求。
source.commit(partition,owner,offset) -> request字符串；来源会在未来某时接纳或拒绝，回复可以丢失。
source.release(partition,owner) -> request字符串；来源只在已持久确认前沿覆盖本owner已交付的完整批次后接纳；
接纳即移除本采集端owned，不会在旧owner下再交付新数据；响应可丢失。
source.poll_events() -> 当前可交付事件列表，取走本轮事件；不产生无限忙等待。
source.check(request) -> {state:"pending"|"accepted"|"rejected",partition,owner,offset可选}，
查询真实接纳结果，回包丢失不等于pending；已结束请求在对应所有权退出后允许遗忘，不能靠永久请求历史重建。
source.fetch取消不提供；begin_revoke事件之后未完成的旧fetch仍会完整交付。

事件：
{kind:"fragment",request,partition,owner,batch,first,count,part,parts,rows,digest}，
batch身份是(owner,batch)，first为批首offset，count为记录数，offset从first连续；
rows=[{offset,body}]，每条body为UTF-8字符串。part在[0,parts)，同批part可乱序重投且原文一致，
各part内部可任意顺序，同批完整分片的offset恰覆盖[first,first+count)，不重复。
digest是按offset排序后的body数组的JSON紧凑UTF8编码SHA256；每批各片元数据一致。
{kind:"fetch_done",request,partition,owner}表示该拉取的所有分片都已交付；同fetch可返回多完整批次或无批次。
合法来源保证没有跨批重叠或永久缺offset，但不同fetch的分片、确认回复和其他分区可任意交错。
{kind:"begin_revoke",partition,owner}要求交还该所有权；不再发fetch，已有fetch及归档须继续结清。
{kind:"reply",request,state}仅是响应提示，真正结果以check为准。
所有事件可能重投，过去owner事件可能在该分区已取得新owner后迟到，不得更改新owner状态或归档。

archive.progress() -> [{partition,owner,offset}]给各仍登记owner的持久连续完整归档位点，初始无项即-1。
archive.stage(partition,owner,batch,first,count,part,parts,rows,digest)同步持久暂存一片，
幂等，返回None；它校验身份、元数据一致性及行格式，异常是实际错误不能当成功。
archive.complete(partition,owner,batch) -> bool，片齐且摘要、offset覆盖正确时原子发布该批；
片未齐返回False；完整但内容不合法抛ValueError。已完成重投返回True，无重复业务副作用。
archive.pending() -> [{partition,owner,batch,first,count}]列仍未完整发布的持久批次，允许重开续接。
archive.retire(partition,owner)在来源已确认release accepted或owned不含旧owner后调用，
仅清理该owner暂存与进度管理，不删除已公开业务记录；不可在仍有未完整批次时调用。
archive.read(partition)可独立查询已经完整发布的{offset,body}列表，无分片半成品。
本轮可扩展现有archive.py增加NewArchive实现上述端口（供真实演示和验证），不能在Pump绕过端口。
来源非答案夹具已提供于source_sdk.py及tests/test_source_sdk.py；仅owned/fetch/commit/release/check/poll_events属于Pump允许的来源端口。
其余assign/deliver_batch/finish_fetch/begin_revoke/settle/replay_events/retained_offsets只供测试操作者控制源端事件，Pump不得调用；不要求真实Kafka。

重开Pump后先按source.owned与archive.progress/pending对账：归档领先来源时补确认，不重复归档；
来源committed领先归档属于协议错误，必须抛RuntimeError，不猜测丢记录。
每owner同时最多一个commit请求，未完成时仍可拉取和归档其后续批次；结束后可继续确认更高前沿。
若begin_revoke后Pump被丢弃，SourceSDK.owned项另含revoking:true，重开不得重新拉取该owner。
fetch在途状态由source.owned项的fetch_request提供（None或request），重开不能重复fetch；
commit_request及release_request同样由owned提供，可用check核实响应丢失状态。
revoking且无在途fetch、无pending批、归档前沿已由来源确认时才发release。
只要owned有空闲非revoking分区，一轮step应轮询它们一次，不等待任何pending request。
同一step处理poll_events所得全部有限事件后再推进确认与交还；不能逐条回复造成同批中间状态外泄。
step无法推进任何动作则False；没有后台定时器要求，不得循环等待未来事件。

来源接纳commit会允许删除<=offset原始记录，这是真实不可逆边界。回包丢失时以check和owned
核实成功；旧owner的accepted不能当作新owner确认。归档progress必须按连续完整批前沿，不能只取max。
实际进程退出仅发生在公开端口调用之间，端口调用本身成功即持久；不要求任意字节崩溃恢复。
只实现此采集所有权交接，不做管理平台、分布式协调服务或UI。
保留原测试及运行清单格式，扩展verify/demo实际跑分片、转交与丢回复场景。
