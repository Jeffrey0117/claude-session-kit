// node example.js — self-contained demo (fake SDK, no API cost).
// Real wiring with @anthropic-ai/claude-agent-sdk is at the bottom.
'use strict'

const { SessionPool } = require('./index.js')

// --- fake SDK so the demo runs offline --------------------------------------
const fakeSdk = {
  query({ prompt, options }) {
    let closed = false
    let turn = 0
    const gen = (async function* () {
      yield { type: 'system', subtype: 'init', session_id: options.resume || 'sdk-demo-1' }
      for await (const userMsg of prompt) {
        if (closed) break
        turn += 1
        const text = typeof userMsg.message.content === 'string'
          ? userMsg.message.content
          : '[blocks]'
        yield { type: 'assistant', message: { role: 'assistant', content: `(demo reply to: ${text})` } }
        yield {
          type: 'result', subtype: 'success', session_id: 'sdk-demo-1',
          usage: {
            input_tokens: 120, output_tokens: 40,
            cache_read_input_tokens: turn === 1 ? 0 : 9000,
            cache_creation_input_tokens: turn === 1 ? 9000 : 400,
          },
        }
      }
    })()
    gen.interrupt = async () => {}
    gen.close = () => { closed = true }
    return gen
  },
}

async function main() {
  const pool = new SessionPool({
    sdk: fakeSdk,
    max: 4,
    idleMs: 10 * 60_000,
    buildOptions: key => ({ cwd: `/projects/${key}`, permissionMode: 'acceptEdits' }),
    onMessage: (key, msg) => {
      if (msg.type === 'assistant') console.log(`[${key}] assistant:`, msg.message.content)
    },
    onError: (key, err) => console.error(`[${key}] error:`, err.message),
    onClose: (key, reason) => console.log(`[${key}] closed (${reason})`),
  })

  // Same key → same live subprocess → warm prompt cache.
  await pool.send('my-app', 'Refactor the auth module')
  await pool.send('my-app', 'Now add tests for it')

  const s = pool.stats('my-app')
  console.log('turns:', s.turns)
  console.log('cache efficiency:', (s.cacheEfficiency * 100).toFixed(1) + '%') // ~48% here; real sessions climb toward 80%+

  pool.closeAll()
}

main()

/* --- real usage --------------------------------------------------------------

const sdk = await import('@anthropic-ai/claude-agent-sdk')  // inject, don't bundle
const { SessionPool } = require('claude-session-kit')

const pool = new SessionPool({
  sdk,
  max: 4,                      // max live `claude` subprocesses
  idleMs: 15 * 60_000,         // recycle after 15 min silence (resume keeps history)
  buildOptions: projectKey => ({
    cwd: `/home/me/projects/${projectKey}`,
    permissionMode: 'acceptEdits',
    // model, allowedTools, mcpServers, hooks... any sdk.query options
  }),
  onMessage: (key, msg) => {
    if (msg.type === 'stream_event') streamToTelegram(key, msg)
  },
})

// e.g. a Telegram bot: chat/project id is the key — every message after the
// first hits a warm subprocess (no 3-4s cold start, prompt cache intact).
bot.on('message', async ctx => {
  const result = await pool.send(ctx.projectKey, ctx.text)
  await ctx.reply(result.result ?? '(done)')
  console.log('cache efficiency', pool.stats(ctx.projectKey)?.cacheEfficiency)
})

// mid-conversation controls (impossible with `claude -p`):
// const s = await pool.get(key); await s.interrupt(); await s.setModel('opus')

------------------------------------------------------------------------------- */
