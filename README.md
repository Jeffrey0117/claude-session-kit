# claude-session-kit

**常駐 Claude Agent SDK session（streaming-input mode）：讓 `claude` CLI subprocess 跨輪存活，prompt cache 效率從 ~20% 拉到 80%+，附 per-turn 結果、interrupt/setModel 控制、cache 統計、per-project session pool。** 零依賴、SDK 注入、框架無關。

## 為什麼

每則訊息呼叫一次 `sdk.query()`（或 `claude -p --resume`）= 每次 spawn 新的 CLI subprocess。新 process 重新組裝 request（檔案追蹤表 timestamp、git status、task 列表每次都差幾個 byte），Anthropic prompt cache 是**前綴比對**，一個 byte 不同 → 該位置之後全部 miss。

- cache read = **10%** 費用；cache miss → cache write = **125%** 費用
- 實測：per-message respawn 的 cache 效率 ~15-50%；常駐 session 可達 **84%**，每訊息額度消耗差 4-6 倍
- 另外每次 spawn 還吃 **3-4 秒冷啟動**

（數據出處：[tony1223/better-agent-terminal#78](https://github.com/tony1223/better-agent-terminal/issues/78)、[逆向分析文](https://cablate.com/articles/reverse-engineer-claude-agent-sdk-hidden-token-cost/)。pattern 改寫自 better-agent-terminal 的 LiveQuery（MIT），加了 prompt 塑形、cache 統計、idle 回收、session pool。）

這個 kit 用 SDK 的 **streaming-input mode**：每個 session 只呼叫一次 `sdk.query()`，`prompt` 是一個可控的 AsyncIterable，之後每輪訊息 `push` 進去 —— subprocess 常駐、對話 append-only、cache 前綴穩定。副作用是解鎖 SDK 的控制方法（`interrupt` / `stopTask` / `setModel` / `setPermissionMode`，單發模式下依 SDK 契約不可用）。

## 安裝

```bash
npm i github:Jeffrey0117/claude-session-kit   # 或直接複製 index.js（零依賴）
npm i @anthropic-ai/claude-agent-sdk          # SDK 由你注入，kit 不綁版本
```

## 用法

### 單一 session

```js
const { createSession } = require('claude-session-kit')
const sdk = await import('@anthropic-ai/claude-agent-sdk')

const session = createSession({
  sdk,
  options: { cwd: '/path/to/project', permissionMode: 'acceptEdits' }, // 任何 sdk.query options
  idleMs: 15 * 60_000,                        // 閒置 15 分鐘自動回收 subprocess
  onMessage: msg => {                         // 每個 SDK message（stream_event / assistant / result...）
    if (msg.type === 'stream_event') process.stdout.write(msg.event?.delta?.text ?? '')
  },
  onError: err => console.error(err),
  onClose: reason => console.log('closed:', reason), // 'close' | 'idle' | 'ended' | 'error'
})

const result = await session.send('Refactor the auth module')   // resolve 於該輪 result frame
await session.send({ text: '看一下這張圖', images: ['data:image/png;base64,...'] })

// 中途控制（claude -p 做不到的部分）
await session.interrupt()
await session.setModel('claude-opus-4-8')

console.log(session.stats)            // { turns, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens, costUsd }
console.log(session.cacheEfficiency)  // read / (read+write)，健康值 0.8+
console.log(session.sdkSessionId)     // 之後可拿去 options.resume 續接

session.close()
```

### SessionPool（bot / 多專案場景）

一個 key（專案 / 聊天室 / workspace）一條常駐 session。session 因 idle、crash、LRU 淘汰而關閉時，pool 記住它的 `sdkSessionId`，下次同 key 自動帶 `resume` 重建 —— **subprocess 死了對話還在**。

```js
const { createPool } = require('claude-session-kit')

const pool = createPool({
  sdk,
  max: 4,                                  // 最多同時 4 條 subprocess，超過 LRU 淘汰
  idleMs: 15 * 60_000,
  buildOptions: key => ({ cwd: `/projects/${key}`, permissionMode: 'acceptEdits' }), // 可 async
  onMessage: (key, msg) => { /* stream 到 Telegram / UI */ },
  onError: (key, err) => console.error(key, err),
  onClose: (key, reason) => console.log(key, 'closed:', reason),
})

// Telegram bot：chat/project id 當 key，第一則之後全部打在熱 subprocess 上
bot.on('message', async ctx => {
  const result = await pool.send(ctx.projectKey, ctx.text)
  await ctx.reply(result.result ?? '(done)')
})

pool.stats('my-app')   // { turns, ..., cacheEfficiency }
pool.close('my-app')   // 收掉一條（resume id 保留）
pool.closeAll()
```

## API

| 名稱 | 說明 |
|---|---|
| `createSession(opts)` / `new ClaudeSession(opts)` | `opts`: `sdk`*, `options`, `onMessage`, `onError`, `onClose`, `idleMs` |
| `session.send(input)` | `input`: string \| `{text, images:[dataUrl]}` \| SDKUserMessage → `Promise<resultMsg>`（FIFO，可並發排隊） |
| `session.interrupt()` / `stopTask(id)` / `setModel(m)` / `setPermissionMode(m)` | proxy 到 SDK query 控制方法 |
| `session.stats` / `session.cacheEfficiency` / `session.sdkSessionId` / `session.isClosed` | 觀測 |
| `session.close(reason?)` | 收掉 subprocess，拒絕未完成的 send |
| `createPool(opts)` / `new SessionPool(opts)` | `opts`: `sdk`*, `buildOptions(key)`, `max`, `idleMs`, `onMessage(key,msg)`, `onError`, `onClose` |
| `pool.get(key)` / `send(key, input)` / `stats(key)` / `has(key)` / `close(key)` / `closeAll()` / `size` | key 級操作；重建自動 `resume` |
| `buildUserMessage(text, images?)` | 手動塑形 SDKUserMessage |

## 測試

```bash
node --test        # 10 tests：FIFO、stats、close/error 語意、idle、pool 重用/resume/LRU
node example.js    # 離線 demo（fake SDK，不花 API）
```

## 注意

- SDK 一律注入（`opts.sdk`），kit 不 import、不綁版本 —— `claude-agent-sdk` 換版只要 `query()` 契約不變就相容。
- `send()` 的 result message 裡有 `usage` / `modelUsage`，kit 自動累計；`cacheCreation` 接近或大於 `cacheRead` 代表 cache 在浪費，檢查是不是還在 per-message respawn。
- 控制方法在 SDK 不支援時 reject（不會 silently no-op）。

## License

MIT © Jeffrey0117
