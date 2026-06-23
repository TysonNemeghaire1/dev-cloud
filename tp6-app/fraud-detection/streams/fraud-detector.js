/**
 * FraudGuard Fraud Detector — Kafka Streams en Node.js
 * Détecte 3 patterns de fraude :
 * 1. Micro-transactions répétées (> 10 tx < 2€ en 5 minutes)
 * 2. Vélocité élevée (> 20 transactions en 5 minutes, tous montants)
 * 3. IP suspecte (adresse connue de bots)
 *
 * TP6 §2.3 : instrumentation OpenTelemetry (span custom autour de l'analyse).
 */
require('./tracing');   // DOIT être le premier require (initialise OpenTelemetry)
const { Kafka } = require('kafkajs');
const { trace } = require('@opentelemetry/api');

const tracer = trace.getTracer('fraud-detector', 'v2');

const kafka = new Kafka({
  clientId: 'fraudguard-streams-detector',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'fraud-detection-group' });
const producer = kafka.producer();

// ============================================================
// Métriques Prometheus : histogramme de latence Producer → Alerte
// Exposé sur :9102/metrics (scrapé par un PodMonitor).
// ============================================================
const http = require('http');
const client = require('prom-client');
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const detectionLatency = new client.Histogram({
  name: 'fraud_detection_latency_seconds',
  help: 'Latence de détection : de la création de la transaction (producer) à la publication de l\'alerte',
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
  registers: [register],
});

// TP6 : compteur exploité par l'AnalysisTemplate du Canary (success-rate)
const processedTotal = new client.Counter({
  name: 'fraud_detection_processed_total',
  help: 'Nombre de transactions traitées, par statut et version',
  labelNames: ['status', 'version'],
  registers: [register],
});
const APP_VERSION = process.env.APP_VERSION || 'stable';

http.createServer(async (req, res) => {
  if (req.url === '/metrics') {
    res.setHeader('Content-Type', register.contentType);
    res.end(await register.metrics());
  } else {
    res.statusCode = 404;
    res.end();
  }
}).listen(9102, () => console.log('[metrics] endpoint Prometheus sur :9102/metrics'));

// ============================================================
// État en mémoire (en prod : utiliser un State Store Redis ou RocksDB)
// Fenêtre glissante de 5 minutes par compte
// ============================================================
const WINDOW_SIZE_MS = 5 * 60 * 1000;      // 5 minutes en ms
const MICRO_TX_THRESHOLD_AMOUNT = 2.00;    // < 2€ = micro-transaction
const MICRO_TX_THRESHOLD_COUNT = 10;       // > 10 micro-tx en 5 min = fraude
const VELOCITY_THRESHOLD = 20;             // > 20 tx en 5 min = fraude

// Map : account_id → liste de transactions dans la fenêtre courante
const windowedTransactions = new Map();

function pruneExpiredTransactions(accountId) {
  const now = Date.now();
  const txList = windowedTransactions.get(accountId) || [];
  const fresh = txList.filter(tx => now - tx._received_at < WINDOW_SIZE_MS);
  windowedTransactions.set(accountId, fresh);
  return fresh;
}

async function analyzeTransaction(transaction) {
  // TP6 §2.3 : span custom pour l'analyse de fraude
  return await tracer.startActiveSpan('analyze_transaction', async (span) => {
    span.setAttributes({
      'fraud.account_id': transaction.account_id,
      'fraud.tx_amount': transaction.amount,
      'fraud.tx_type': transaction.tx_type,
    });

    try {
      const accountId = transaction.account_id;
      const now = Date.now();

      // Ajouter la transaction à la fenêtre en mémoire
      const txList = pruneExpiredTransactions(accountId);
      txList.push({ ...transaction, _received_at: now });
      windowedTransactions.set(accountId, txList);

      const alerts = [];

      // ---- Pattern 1 : Micro-transactions répétées ----
      const microTxCount = txList.filter(tx => tx.amount < MICRO_TX_THRESHOLD_AMOUNT).length;
      if (microTxCount >= MICRO_TX_THRESHOLD_COUNT) {
        alerts.push({
          alert_type: 'MICRO_TRANSACTION_PATTERN',
          severity: 'HIGH',
          description: `${microTxCount} micro-transactions (< ${MICRO_TX_THRESHOLD_AMOUNT}€) en 5 minutes`,
          micro_tx_count: microTxCount,
          total_amount: txList
            .filter(tx => tx.amount < MICRO_TX_THRESHOLD_AMOUNT)
            .reduce((sum, tx) => sum + tx.amount, 0)
            .toFixed(2),
        });
      }

      // ---- Pattern 2 : Vélocité élevée ----
      if (txList.length >= VELOCITY_THRESHOLD) {
        alerts.push({
          alert_type: 'HIGH_VELOCITY',
          severity: 'CRITICAL',
          description: `${txList.length} transactions en 5 minutes — vélocité anormale`,
          tx_count_5min: txList.length,
        });
      }

      // ---- Pattern 3 : IP suspecte (adresse connue de bots) ----
      const suspiciousIPs = ['185.234.219.45', '31.220.0.0/24'];
      if (suspiciousIPs.includes(transaction.ip_address)) {
        alerts.push({
          alert_type: 'SUSPICIOUS_IP',
          severity: 'MEDIUM',
          description: `Adresse IP suspecte détectée : ${transaction.ip_address}`,
        });
      }

      span.setAttribute('fraud.alerts_count', alerts.length);
      if (alerts.length > 0) {
        span.setAttribute('fraud.alert_severity', alerts[0].severity);
        // Marquer le span comme "intéressant" pour le sampling Tempo
        span.setAttribute('sampling.priority', 1);   // force la capture
      }
      span.setStatus({ code: 1 });   // OK
      processedTotal.inc({ status: 'success', version: APP_VERSION });
      return alerts;
    } catch (err) {
      span.recordException(err);
      span.setStatus({ code: 2, message: err.message });   // ERROR
      processedTotal.inc({ status: 'error', version: APP_VERSION });
      throw err;
    } finally {
      span.end();
    }
  });
}

async function publishAlert(transaction, alert) {
  const fraudAlert = {
    alert_id: `ALERT-${Date.now()}-${transaction.account_id}`,
    account_id: transaction.account_id,
    account_name: transaction.account_name,
    triggering_tx_id: transaction.tx_id,
    triggering_amount: transaction.amount,
    ...alert,
    window_size_minutes: WINDOW_SIZE_MS / 60000,
    detected_at: new Date().toISOString(),
    action_recommended: alert.severity === 'CRITICAL' ? 'BLOCK_ACCOUNT' : 'REVIEW',
  };

  await producer.send({
    topic: 'fraud-alerts',
    messages: [{
      key: transaction.account_id,
      value: JSON.stringify(fraudAlert),
    }],
  });

  // Latence Producer → Alerte : timestamp de la transaction vs maintenant
  const latencySec = (Date.now() - Date.parse(transaction.timestamp)) / 1000;
  if (latencySec >= 0) detectionLatency.observe(latencySec);

  console.log(`[ALERTE ${alert.severity}] ${alert.alert_type} — Compte ${transaction.account_id}`);
  console.log(`  → ${alert.description}`);
}

async function startDetection() {
  await Promise.all([consumer.connect(), producer.connect()]);
  console.log('[FraudGuard] Moteur de détection Kafka Streams démarré');

  await consumer.subscribe({ topics: ['transactions-raw'], fromBeginning: false });

  // Stats de monitoring
  let txProcessed = 0;
  let alertsGenerated = 0;

  setInterval(() => {
    const accountsMonitored = windowedTransactions.size;
    const totalWindowedTx = Array.from(windowedTransactions.values())
      .reduce((sum, list) => sum + list.length, 0);
    console.log(`[STATS] Traitées: ${txProcessed} tx | Alertes: ${alertsGenerated} | Comptes surveillés: ${accountsMonitored} | En fenêtre: ${totalWindowedTx} tx`);
  }, 30000);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const transaction = JSON.parse(message.value.toString());
      txProcessed++;

      const alerts = await analyzeTransaction(transaction);

      for (const alert of alerts) {
        await publishAlert(transaction, alert);
        alertsGenerated++;
      }
    },
  });
}

process.on('SIGTERM', async () => {
  await Promise.all([consumer.disconnect(), producer.disconnect()]);
  process.exit(0);
});

startDetection().catch(console.error);
