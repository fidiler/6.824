
### commit和apply的区别（论文中commitIndex和lastApplied的区别）

日志复制到过半服务器上，commit该日志，**commitIndex就会更新**，此时称该日志条目为 **commited**

日志被commit后，才会apply，在apply的时候，比较 `commitIndex > lastApplied`, 这个比较实际上是在确定是否有需要应用的日志，即：提交的日志条目比应用的日志条目多，那么就会递增 `lastApplied`, 然后将该日志条目应用到状态机

leader将该日志条目复制到过半服务器上，该日志条目就会被提交

### 什么时候更新commitIndex

论文中Figure 2指出：N > commitIndex, and majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N

这里实际上是三个并列的条件：

1. N > commitIndex 这里的N实际上就是要复制的日志的index值

2. majority of matchIndex[i] >= N, 怎么得出？在发送`AppendEntries` 成功后更新所有的matchIndex后进行计算

3. log[N].term = currentTerm

### nextIndex值的维护

**初始化**

Leader将所有nextIndex值都初始化为自己最后一个日志条目的index + 1（其实也就是 `len(rf.logs)` 的长度）

**追加失败**

如果此次`AppendEntries`失败，那么leader会减小`nextIndex`值重试。最终leader和follower的`nextIndex`会在某个位置达成一致（或者从头开始）。

> 关于优化：follower维护冲突条目的term和这个term对应的第一个index。追加失败时，leader跳过这个term所有的日志条目减小nextIndex。

**追加成功**

## 提交之前任期的日志条目问题

leader如果要复制之前任期里的日志，必须使用当前新的任期号