const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');

const secretClient = new SecretManagerServiceClient();

/**
 * FinSecure Payment Processor
 * Cloud Function déclenchée par un message Pub/Sub à chaque transaction validée.
 */
exports.processPayment = async (message, context) => {
  // Décoder le message Pub/Sub (encodé en base64)
  const payload = message.data
    ? Buffer.from(message.data, 'base64').toString()
    : '{}';

  let transaction;
  try {
    transaction = JSON.parse(payload);
  } catch (err) {
    console.error('Payload JSON invalide :', payload);
    // Ne pas throw : un throw = retry automatique par Pub/Sub
    // Pour un message corrompu, on log et on ACK (évite la boucle infinie)
    return;
  }

  console.log(`Traitement transaction : ${transaction.transaction_id}`);
  console.log(`Montant : ${transaction.amount} ${transaction.currency}`);
  console.log(`Marchand : ${transaction.merchant_id}`);

  // Valider les champs obligatoires
  const requiredFields = ['transaction_id', 'amount', 'currency', 'status', 'merchant_id'];
  for (const field of requiredFields) {
    if (!transaction[field]) {
      console.error(`Champ manquant : ${field}`);
      return;   // ACK le message invalide sans retry
    }
  }

  // Accéder au secret DB depuis Secret Manager (sans clé JSON grâce à Workload Identity)
  const projectId = process.env.GCP_PROJECT;
  const [version] = await secretClient.accessSecretVersion({
    name: `projects/${projectId}/secrets/finsecure-db-password/versions/latest`,
  });
  const dbPassword = version.payload.data.toString();
  console.log(`Connexion DB avec secret récupéré : ${dbPassword.substring(0, 4)}****`);

  // Simulation : enregistrement en base de données
  await simulateDbWrite(transaction);

  // Log d'audit structuré (lisible dans Cloud Logging)
  console.log(JSON.stringify({
    severity: 'INFO',
    event_type: 'PAYMENT_PROCESSED',
    transaction_id: transaction.transaction_id,
    amount: transaction.amount,
    currency: transaction.currency,
    merchant_id: transaction.merchant_id,
    processed_at: new Date().toISOString(),
  }));
};

async function simulateDbWrite(transaction) {
  // En production : connexion à Cloud SQL via Cloud SQL Auth Proxy
  return new Promise(resolve => setTimeout(resolve, 50));
}
