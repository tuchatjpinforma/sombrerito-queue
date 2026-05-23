require('dotenv').config();
const express           = require('express');
const { Queue }         = require('bullmq');
const IORedis           = require('ioredis');
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter }   = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter }  = require('@bull-board/express');

const redisConn = new IORedis({
  host:                 process.env.REDIS_HOST,
  port:                 +process.env.REDIS_PORT,
  password:             process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
  enableReadyCheck:     false
});

redisConn.on('connect', () => console.log('✅ Redis conectado'));
redisConn.on('error',   (e) => console.error('❌ Redis error:', e.message));

const mensajesQueue = new Queue('sombrerito-messages', {
  connection: redisConn,
  defaultJobOptions: {
    attempts:         3,
    backoff:          { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 500 },
    removeOnFail:     { count: 200 }
  }
});

const boardAdapter = new ExpressAdapter();
boardAdapter.setBasePath('/admin/queues');
createBullBoard({
  queues:        [new BullMQAdapter(mensajesQueue)],
  serverAdapter: boardAdapter
});

const app = express();
app.use(express.json({ limit: '5mb' }));
app.use('/admin/queues', boardAdapter.getRouter());

app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const body  = req.body;
    const event = body?.event;

    if (event !== 'messages.upsert') {
      console.log(`⏭  Evento ignorado: ${event}`);
      return;
    }

    const msg = body?.data;
    if (!msg || msg?.key?.fromMe === true) return;

    const remoteJid = msg?.key?.remoteJid || '';
    if (remoteJid.includes('@g.us')) {
      console.log(`⏭  Grupo ignorado: ${remoteJid}`);
      return;
    }

    const job = await mensajesQueue.add('mensaje-entrante', { rawBody: body });
    console.log(`📨 Job #${job.id} encolado: ${remoteJid}`);

  } catch (err) {
    console.error('❌ Error al encolar:', err.message);
  }
});

app.get('/health', async (_, res) => {
  try {
    await redisConn.ping();
    const counts = await mensajesQueue.getJobCounts();
    res.json({ status: 'ok', redis: 'connected', queue: counts });
  } catch (e) {
    res.status(500).json({ status: 'error', redis: e.message });
  }
});

const PORT = process.env.PORT || 3002;
app.listen(PORT, () =>
  console.log(`🎩 Queue Server corriendo en :${PORT}`)
);
