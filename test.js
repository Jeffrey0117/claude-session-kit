// node --test — exercises ClaudeSession + SessionPool against a fake SDK.
'use strict'

const test = require('node:test')
const assert = require('node:assert')
const { ClaudeSession, SessionPool, buildUserMessage } = require('./index.js')

// makeFakeSdk: mimics sdk.query() streaming-input contract. For every user
// message read off the prompt iterable it yields an assistant message then a
// result message. Exposes spawn/interrupt counters for assertions.
function makeFakeSdk({ failAfterTurns = Infinity } = {}) {
  const state = { spawns: 0, interrupts: 0, closed: 0, lastOptions: null }
  const sdk = {
    query({ prompt, options }) {
      state.spawns += 1
      state.lastOptions = options
      let closed = false
      let turn = 0
      const gen = (async function* () {
        yield { type: 'system', subtype: 'init', session_id: options.resume || `sdk-${state.spawns}` }
        for await (const userMsg of prompt) {
          if (closed) break
          turn += 1
          if (turn > failAfterTurns) throw new Error('boom')
          yield { type: 'assistant', message: { role: 'assistant', content: `echo:${JSON.stringify(userMsg.message.content)}` } }
          yield {
            type: 'result', subtype: 'success', session_id: options.resume || `sdk-${state.spawns}`,
            usage: {
              input_tokens: 100, output_tokens: 50,
              cache_read_input_tokens: turn === 1 ? 0 : 8000,
              cache_creation_input_tokens: turn === 1 ? 8000 : 500,
            },
            total_cost_usd: 0.01,
          }
        }
      })()
      gen.interrupt = async () => { state.interrupts += 1 }
      gen.close = () => { closed = true; state.closed += 1 }
      return gen
    },
  }
  return { sdk, state }
}

test('send resolves with the turn result and stats accumulate', async () => {
  const { sdk } = makeFakeSdk()
  const messages = []
  const s = new ClaudeSession({ sdk, options: {}, onMessage: m => messages.push(m.type) })

  const r1 = await s.send('hello')
  assert.strictEqual(r1.type, 'result')
  const r2 = await s.send('again')
  assert.strictEqual(r2.type, 'result')

  assert.strictEqual(s.stats.turns, 2)
  assert.strictEqual(s.stats.inputTokens, 200)
  assert.strictEqual(s.stats.cacheReadTokens, 8000)
  assert.strictEqual(s.stats.cacheCreationTokens, 8500)
  assert.ok(s.cacheEfficiency > 0.45 && s.cacheEfficiency < 0.5)
  assert.ok(messages.includes('assistant'))
  assert.strictEqual(s.sdkSessionId, 'sdk-1')
  s.close()
})

test('multiple pending sends resolve FIFO', async () => {
  const { sdk } = makeFakeSdk()
  const s = new ClaudeSession({ sdk, onMessage: () => {} })
  const [a, b, c] = await Promise.all([s.send('a'), s.send('b'), s.send('c')])
  assert.strictEqual(s.stats.turns, 3)
  for (const r of [a, b, c]) assert.strictEqual(r.type, 'result')
  s.close()
})

test('close rejects pending sends and fires onClose once', async () => {
  const { sdk, state } = makeFakeSdk()
  const closes = []
  const s = new ClaudeSession({ sdk, onMessage: () => {}, onClose: r => closes.push(r) })
  await s.send('warm')
  // Queue a send, then close before the fake ever reads it: close first so
  // the iterator exits without yielding a result for it.
  s.close()
  await assert.rejects(() => s.send('never'), /closed/)
  assert.deepStrictEqual(closes, ['close'])
  assert.strictEqual(state.closed, 1)
  assert.strictEqual(s.isClosed, true)
})

test('stream error rejects pending sends and closes with reason error', async () => {
  const { sdk } = makeFakeSdk({ failAfterTurns: 1 })
  const errors = []
  const closes = []
  const s = new ClaudeSession({ sdk, onMessage: () => {}, onError: e => errors.push(e.message), onClose: r => closes.push(r) })
  await s.send('ok')
  await assert.rejects(() => s.send('kaboom'), /boom|closed/)
  assert.ok(errors.some(m => m.includes('boom')))
  assert.deepStrictEqual(closes, ['error'])
})

test('idle timeout auto-closes with reason idle', async () => {
  const { sdk } = makeFakeSdk()
  const closes = []
  const s = new ClaudeSession({ sdk, idleMs: 50, onMessage: () => {}, onClose: r => closes.push(r) })
  await s.send('one')
  await new Promise(r => setTimeout(r, 120))
  assert.strictEqual(s.isClosed, true)
  assert.deepStrictEqual(closes, ['idle'])
})

test('interrupt proxies to the SDK query', async () => {
  const { sdk, state } = makeFakeSdk()
  const s = new ClaudeSession({ sdk, onMessage: () => {} })
  await s.interrupt()
  assert.strictEqual(state.interrupts, 1)
  s.close()
})

test('buildUserMessage shapes text and image inputs', () => {
  const plain = buildUserMessage('hi')
  assert.deepStrictEqual(plain, { type: 'user', message: { role: 'user', content: 'hi' } })
  const withImg = buildUserMessage('look', ['data:image/png;base64,AAAA'])
  const blocks = withImg.message.content
  assert.strictEqual(blocks.length, 2)
  assert.strictEqual(blocks[0].type, 'image')
  assert.strictEqual(blocks[0].source.media_type, 'image/png')
  assert.strictEqual(blocks[1].text, 'look')
})

test('pool reuses one session per key', async () => {
  const { sdk, state } = makeFakeSdk()
  const pool = new SessionPool({ sdk, buildOptions: () => ({}), onMessage: () => {} })
  await pool.send('projA', 'msg1')
  await pool.send('projA', 'msg2')
  await pool.send('projB', 'msg1')
  assert.strictEqual(state.spawns, 2) // one subprocess per key, not per message
  assert.strictEqual(pool.size, 2)
  assert.ok(pool.stats('projA').turns === 2)
  pool.closeAll()
})

test('pool rebuilds a closed session with resume id', async () => {
  const { sdk, state } = makeFakeSdk()
  const pool = new SessionPool({ sdk, buildOptions: () => ({}), onMessage: () => {} })
  await pool.send('projA', 'first')
  pool.close('projA')
  await new Promise(r => setImmediate(r))
  await pool.send('projA', 'second')
  assert.strictEqual(state.spawns, 2)
  assert.strictEqual(state.lastOptions.resume, 'sdk-1') // conversation carried over
  pool.closeAll()
})

test('pool LRU-evicts beyond max and keeps resume ids', async () => {
  const { sdk, state } = makeFakeSdk()
  const closes = []
  const pool = new SessionPool({ sdk, max: 2, buildOptions: () => ({}), onMessage: () => {}, onClose: (k, r) => closes.push([k, r]) })
  await pool.send('a', '1')
  await pool.send('b', '1')
  await pool.send('c', '1') // evicts 'a'
  assert.strictEqual(pool.size, 2)
  assert.ok(closes.some(([k, r]) => k === 'a' && r === 'evicted'))
  await pool.send('a', '2') // rebuilds with resume
  assert.strictEqual(state.lastOptions.resume, 'sdk-1')
  pool.closeAll()
})
