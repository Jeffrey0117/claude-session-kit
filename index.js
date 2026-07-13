// ============================================================================
// claude-session-kit — Persistent Claude Agent SDK sessions (streaming-input
// mode): keep the CLI subprocess alive across turns for ~8x prompt-cache
// efficiency, with per-turn results, interrupt/setModel, cache stats, and a
// per-project session pool.
// ----------------------------------------------------------------------------
// 零依賴（node 內建）。SDK 一律注入（opts.sdk），kit 不自己 import。
//
// Why: calling sdk.query() once per user message spawns a fresh `claude` CLI
// subprocess each time. The new process re-assembles the request (file
// tracking tables, git status, task lists) with slightly different bytes, so
// the Anthropic prompt-cache prefix match fails — cache write costs 125% of
// base input price, cache read only 10%. Measured efficiency drops from ~85%
// to ~49% (see tony1223/better-agent-terminal#78). Streaming-input mode keeps
// ONE subprocess alive per session; every later turn is appended to a stable
// prefix, so the cache keeps hitting and there is no 3-4s cold start.
//
// Pattern adapted from better-agent-terminal's LiveQuery (MIT), rewritten and
// extended: string/image prompt shaping, per-turn cache stats, idle auto-
// close, and a keyed SessionPool with resume-on-rebuild.
// ============================================================================

'use strict'

const TURN_END_TYPES = new Set(['result'])

// --- prompt shaping ---------------------------------------------------------

// dataUrlToContentBlock: 'data:image/png;base64,AAAA' → Anthropic image block.
function dataUrlToContentBlock(dataUrl) {
  if (typeof dataUrl !== 'string') return null
  const m = /^data:(image\/[a-z0-9.+-]+);base64,(.+)$/i.exec(dataUrl.trim())
  if (!m) return null
  return { type: 'image', source: { type: 'base64', media_type: m[1].toLowerCase(), data: m[2] } }
}

// buildUserMessage: shape an SDKUserMessage from text + optional image data
// URLs. A single content-block array carrying images first, then text.
function buildUserMessage(text, images) {
  const prompt = typeof text === 'string' ? text : ''
  const imageList = Array.isArray(images) ? images : null
  if (imageList && imageList.length > 0) {
    const imageBlocks = imageList.map(dataUrlToContentBlock).filter(Boolean)
    if (imageBlocks.length > 0) {
      const content = [
        ...imageBlocks,
        ...(prompt ? [{ type: 'text', text: prompt }] : []),
      ]
      return { type: 'user', message: { role: 'user', content } }
    }
  }
  return { type: 'user', message: { role: 'user', content: prompt || ' ' } }
}

// normalizeInput: accept a plain string, {text, images}, or a full
// SDKUserMessage and always return an SDKUserMessage.
function normalizeInput(input) {
  if (typeof input === 'string') return buildUserMessage(input)
  if (input && typeof input === 'object') {
    if (input.type === 'user' && input.message) return input
    if ('text' in input || 'images' in input) return buildUserMessage(input.text, input.images)
  }
  throw new TypeError('send() expects a string, {text, images}, or an SDKUserMessage')
}

// --- per-turn usage extraction ----------------------------------------------

// extractUsage: read token counts off a result message. The SDK reports
// snake_case under msg.usage and camelCase under msg.modelUsage[model]; take
// whichever is present.
function extractUsage(msg) {
  const out = { input: 0, output: 0, cacheRead: 0, cacheCreation: 0, costUsd: 0 }
  const u = msg && typeof msg === 'object' ? msg.usage : null
  if (u && typeof u === 'object') {
    out.input += u.input_tokens || 0
    out.output += u.output_tokens || 0
    out.cacheRead += u.cache_read_input_tokens || 0
    out.cacheCreation += u.cache_creation_input_tokens || 0
  }
  const mu = msg && typeof msg === 'object' ? msg.modelUsage : null
  if ((!u || (!out.cacheRead && !out.cacheCreation)) && mu && typeof mu === 'object') {
    for (const per of Object.values(mu)) {
      if (!per || typeof per !== 'object') continue
      out.input += per.inputTokens || 0
      out.output += per.outputTokens || 0
      out.cacheRead += per.cacheReadInputTokens || 0
      out.cacheCreation += per.cacheCreationInputTokens || 0
      out.costUsd += per.costUSD || 0
    }
  }
  if (typeof msg?.total_cost_usd === 'number') out.costUsd = msg.total_cost_usd
  return out
}

// --- ClaudeSession -----------------------------------------------------------

class ClaudeSession {
  /**
   * @param {object} opts
   * @param {object} opts.sdk        injected @anthropic-ai/claude-agent-sdk (needs .query)
   * @param {object} [opts.options]  sdk.query options (cwd, model, resume, permissionMode, ...)
   * @param {function} [opts.onMessage]  every SDK message (assistant, stream_event, result, ...)
   * @param {function} [opts.onError]    stream/callback errors
   * @param {function} [opts.onClose]    called once with a reason: 'close' | 'idle' | 'ended' | 'error'
   * @param {number} [opts.idleMs]   auto-close after this long with no activity (0 = never)
   */
  constructor({ sdk, options = {}, onMessage, onError, onClose, idleMs = 0 } = {}) {
    if (!sdk || typeof sdk.query !== 'function') {
      throw new Error('ClaudeSession: opts.sdk with a query() function is required (inject the Agent SDK)')
    }
    this._queue = []
    this._waker = null
    this._closed = false
    this._closeReason = null
    this._turnDeferreds = []
    this._onMessage = typeof onMessage === 'function' ? onMessage : () => {}
    this._onError = typeof onError === 'function' ? onError : () => {}
    this._onClose = typeof onClose === 'function' ? onClose : () => {}
    this._idleMs = Number.isFinite(idleMs) && idleMs > 0 ? idleMs : 0
    this._idleTimer = null

    this.sdkSessionId = typeof options.resume === 'string' ? options.resume : null
    this.stats = { turns: 0, inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, cacheCreationTokens: 0, costUsd: 0 }

    const self = this
    const promptIterable = {
      async *[Symbol.asyncIterator]() {
        while (!self._closed) {
          if (self._queue.length > 0) {
            yield self._queue.shift()
            continue
          }
          await new Promise(resolve => { self._waker = resolve })
          self._waker = null
        }
      },
    }
    this._queryArgs = { prompt: promptIterable, options }
    this.query = sdk.query(this._queryArgs)
    this._touch()
    this._loopPromise = this._drain()
  }

  get isClosed() { return this._closed }

  // cacheEfficiency: read / (read + write). The number issue #78 measures —
  // healthy persistent sessions sit around 0.8+; per-message respawn ~0.2-0.5.
  get cacheEfficiency() {
    const { cacheReadTokens, cacheCreationTokens } = this.stats
    const total = cacheReadTokens + cacheCreationTokens
    return total > 0 ? cacheReadTokens / total : null
  }

  _touch() {
    if (!this._idleMs || this._closed) return
    if (this._idleTimer) clearTimeout(this._idleTimer)
    this._idleTimer = setTimeout(() => this.close('idle'), this._idleMs)
    if (typeof this._idleTimer.unref === 'function') this._idleTimer.unref()
  }

  async _drain() {
    try {
      for await (const msg of this.query) {
        if (this._closed) break
        this._touch()
        if (msg && typeof msg.session_id === 'string') this.sdkSessionId = msg.session_id
        try { this._onMessage(msg) } catch (err) { this._onError(err) }
        if (msg && typeof msg === 'object' && TURN_END_TYPES.has(msg.type)) {
          const usage = extractUsage(msg)
          this.stats.turns += 1
          this.stats.inputTokens += usage.input
          this.stats.outputTokens += usage.output
          this.stats.cacheReadTokens += usage.cacheRead
          this.stats.cacheCreationTokens += usage.cacheCreation
          this.stats.costUsd += usage.costUsd
          const d = this._turnDeferreds.shift()
          if (d) d.resolve(msg)
        }
      }
      this._settleClosed('ended')
    } catch (err) {
      this._onError(err)
      const wrapped = err instanceof Error ? err : new Error(String(err))
      for (const d of this._turnDeferreds) d.reject(wrapped)
      this._turnDeferreds.length = 0
      this._settleClosed('error')
    }
  }

  _settleClosed(reason) {
    if (this._closeReason) return
    this._closeReason = reason
    this._closed = true
    if (this._idleTimer) { clearTimeout(this._idleTimer); this._idleTimer = null }
    if (this._waker) { this._waker(); this._waker = null }
    const closedErr = new Error(`ClaudeSession closed (${reason}) before turn completed`)
    for (const d of this._turnDeferreds) d.reject(closedErr)
    this._turnDeferreds.length = 0
    try { this._onClose(reason) } catch (err) { this._onError(err) }
  }

  // send: push one user turn, resolve with that turn's 'result' message.
  // Accepts a string, {text, images: [dataUrl]}, or a full SDKUserMessage.
  send(input) {
    if (this._closed) return Promise.reject(new Error('ClaudeSession is closed'))
    let userMessage
    try { userMessage = normalizeInput(input) } catch (err) { return Promise.reject(err) }
    this._touch()
    return new Promise((resolve, reject) => {
      this._turnDeferreds.push({ resolve, reject })
      this._queue.push(userMessage)
      if (this._waker) this._waker()
    })
  }

  _control(name, ...args) {
    if (this._closed) return Promise.reject(new Error('ClaudeSession is closed'))
    if (typeof this.query?.[name] !== 'function') {
      return Promise.reject(new Error(`${name} not supported by this SDK build`))
    }
    this._touch()
    return this.query[name](...args)
  }

  interrupt() { return this._control('interrupt') }
  stopTask(taskId) { return this._control('stopTask', taskId) }
  setModel(model) { return this._control('setModel', model) }
  setPermissionMode(mode) { return this._control('setPermissionMode', mode) }

  close(reason = 'close') {
    if (this._closed) return
    // Mark first so the drain loop and iterator exit promptly.
    this._settleClosed(reason)
    try { this.query?.close?.() } catch { /* subprocess teardown is best-effort */ }
  }
}

// --- SessionPool -------------------------------------------------------------

// SessionPool: one persistent session per key (project / chat / workspace).
// Remembers each key's sdkSessionId so a session that idled out, crashed, or
// was LRU-evicted is transparently rebuilt with `resume` — the conversation
// survives even though the subprocess didn't.
class SessionPool {
  /**
   * @param {object} opts
   * @param {object} opts.sdk            injected Agent SDK
   * @param {function} [opts.buildOptions] (key) → sdk.query options (may be async)
   * @param {number} [opts.max]          max live subprocesses (LRU-evict beyond; default 4)
   * @param {number} [opts.idleMs]       per-session idle auto-close (default 15 min)
   * @param {function} [opts.onMessage]  (key, msg)
   * @param {function} [opts.onError]    (key, err)
   * @param {function} [opts.onClose]    (key, reason)
   */
  constructor({ sdk, buildOptions, max = 4, idleMs = 15 * 60_000, onMessage, onError, onClose } = {}) {
    if (!sdk || typeof sdk.query !== 'function') {
      throw new Error('SessionPool: opts.sdk with a query() function is required')
    }
    this._sdk = sdk
    this._buildOptions = typeof buildOptions === 'function' ? buildOptions : () => ({})
    this._max = Math.max(1, max)
    this._idleMs = idleMs
    this._onMessage = typeof onMessage === 'function' ? onMessage : () => {}
    this._onError = typeof onError === 'function' ? onError : () => {}
    this._onClose = typeof onClose === 'function' ? onClose : () => {}
    this._entries = new Map() // key → { session }
    this._resumeIds = new Map() // key → last sdkSessionId (survives entry removal)
  }

  get size() { return this._entries.size }

  has(key) {
    const e = this._entries.get(key)
    return Boolean(e && !e.session.isClosed)
  }

  // get: return the live session for key, creating (and resuming) if needed.
  async get(key) {
    const existing = this._entries.get(key)
    if (existing && !existing.session.isClosed) {
      // refresh LRU order
      this._entries.delete(key)
      this._entries.set(key, existing)
      return existing.session
    }
    if (existing) this._entries.delete(key)

    const options = { ...(await this._buildOptions(key)) }
    const rememberedId = this._resumeIds.get(key)
    if (rememberedId && options.resume === undefined) options.resume = rememberedId

    const session = new ClaudeSession({
      sdk: this._sdk,
      options,
      idleMs: this._idleMs,
      onMessage: msg => this._onMessage(key, msg),
      onError: err => this._onError(key, err),
      onClose: reason => {
        if (session.sdkSessionId) this._resumeIds.set(key, session.sdkSessionId)
        const cur = this._entries.get(key)
        if (cur && cur.session === session) this._entries.delete(key)
        this._onClose(key, reason)
      },
    })
    this._entries.set(key, { session })
    this._evict()
    return session
  }

  _evict() {
    while (this._entries.size > this._max) {
      const oldestKey = this._entries.keys().next().value
      const entry = this._entries.get(oldestKey)
      this._entries.delete(oldestKey)
      entry.session.close('evicted') // onClose stashes the resume id
    }
  }

  // send: convenience — get-or-create the key's session and push one turn.
  async send(key, input) {
    const session = await this.get(key)
    return session.send(input)
  }

  stats(key) {
    const e = this._entries.get(key)
    return e ? { ...e.session.stats, cacheEfficiency: e.session.cacheEfficiency } : null
  }

  close(key) {
    const e = this._entries.get(key)
    if (e) e.session.close()
  }

  closeAll() {
    for (const { session } of this._entries.values()) session.close()
    this._entries.clear()
  }
}

// --- exports ------------------------------------------------------------------

function createSession(opts) { return new ClaudeSession(opts) }
function createPool(opts) { return new SessionPool(opts) }

module.exports = {
  ClaudeSession,
  SessionPool,
  createSession,
  createPool,
  buildUserMessage,
}
