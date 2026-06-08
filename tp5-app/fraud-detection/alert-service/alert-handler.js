/**
 * FraudGuard Alert Handler
 * Consomme les alertes Kafka et déclenche les actions :
 * - Bloquer le compte (sévérité CRITICAL)
 * - Notifier le Risk Manager (sévérité HIGH)
 * - Enregistrer pour audit (toutes les alertes)
 */
const { Kafka } = require('kafkajs');
const { Firestore } = require('@google-cloud/firestore');

const kafka = new Kafka({
  clientId: 'fraudguard-alert-handler',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'alert-handler-group' });
const db = new Firestore({ projectId: process.env.GCP_PROJECT });

async function processAlert(alert) {
  console.log(`[ALERT HANDLER] Traitement : ${alert.alert_type} — ${alert.account_id}`);

  // 1. Enregistrer l'alerte dans Firestore pour audit
  await db.collection('fraud_alerts').doc(alert.alert_id).set({
    ...alert,
    processed_at: Firestore.Timestamp.now(),
    status: 'PENDING_REVIEW',
  });

  // 2. Action selon la sévérité
  if (alert.severity === 'CRITICAL') {
    // Bloquer le compte immédiatement
    await blockAccount(alert.account_id, alert.alert_id);
    await notifyRiskManager(alert, 'URGENT: Compte bloqué automatiquement');

  } else if (alert.severity === 'HIGH') {
    // Notifier le Risk Manager pour revue manuelle
    await notifyRiskManager(alert, 'Action requise : pattern de fraude détecté');
    // Limiter les transactions (au lieu de bloquer)
    await limitAccountTransactions(alert.account_id, 50);   // Max 50€ par transaction

  } else if (alert.severity === 'MEDIUM') {
    // Enregistrer pour analyse batch par Airflow
    console.log(`[AUDIT] Alerte MEDIUM enregistrée pour revue Airflow quotidienne`);
  }
}

async function blockAccount(accountId, alertId) {
  // En production : appel à l'API Core Banking
  console.log(`[ACTION] BLOCAGE du compte ${accountId} — Alerte ${alertId}`);
  await db.collection('blocked_accounts').doc(accountId).set({
    blocked_at: Firestore.Timestamp.now(),
    reason: alertId,
    status: 'BLOCKED',
  });
}

async function limitAccountTransactions(accountId, maxAmount) {
  console.log(`[ACTION] Limitation du compte ${accountId} à ${maxAmount}€/transaction`);
}

async function notifyRiskManager(alert, message) {
  console.log(`[NOTIFICATION] Risk Manager : ${message}`);
  console.log(`  Compte: ${alert.account_id} | Type: ${alert.alert_type}`);
  // En prod : email/SMS/Slack via API
}

async function startAlertHandling() {
  await consumer.connect();
  await consumer.subscribe({ topics: ['fraud-alerts'], fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const alert = JSON.parse(message.value.toString());
      await processAlert(alert);
    },
  });
}

process.on('SIGTERM', async () => { await consumer.disconnect(); process.exit(0); });
startAlertHandling().catch(console.error);
