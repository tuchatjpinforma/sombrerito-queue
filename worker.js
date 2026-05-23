require('dotenv').config();
const { Worker, MetricsTime } = require('bullmq');
const IORedis                  = require('ioredis');

const redisConn = new IORedis({
  host:                 process.env.REDIS_HOST,
  port:                 +process.env.REDIS_PORT,
  password:             process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
  enableReadyCheck:     false
});

const N8N_URL = process.env.N8N_INTERNAL_URL;

if (!N8N_URL) {
  console.error('❌ N8N_INTERNAL_URL no definido en .env');
  process.exit(1);
}

const worker = new Worker(
  'sombrerito-messages',
  async (job) => {
    const { rawBody } = job.data;
    const remoteJid   = rawBody?.data?.key?.remoteJid || 'desconocido';
    console.log(`⚙️  Job #${job.id} → ${remoteJid}`);

    const res = await fetch(N8N_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(rawBody),
      signal:  AbortSignal.timeout(45000)
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(`n8n ${res.status}: ${txt.slice(0, 100)}`);
    }

    console.log(`✅ Job #${job.id} completado`);
  },
  {
    connection:  redisConn,
    concurrency: 15,
    metrics:     { maxDataPoints: MetricsTime.ONE_WEEK }
  }
);

worker.on('completed', (job) =>
  console.log(`✅ Completado: #${job.id}`)
);
worker.on('failed', (job, err) =>
  console.error(`❌ Fallido: #${job?.id} — ${err.message}`)
);
worker.on('error', (err) =>
  console.error('Worker error:', err.message)
);

console.log('👷 Worker BullMQ listo → escuchando sombrerito-messages');
