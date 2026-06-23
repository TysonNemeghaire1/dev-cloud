/**
 * Instrumentation OpenTelemetry pour le fraud-detector
 * Capture les spans : Kafka consume → analyze → publish alert
 *
 * API OpenTelemetry JS SDK ≥ 2.x (2026) :
 *  - `Resource` est remplacé par `resourceFromAttributes()`
 *  - `SemanticResourceAttributes` est remplacé par les constantes `ATTR_*`
 */
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-grpc');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} = require('@opentelemetry/semantic-conventions');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');

const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    [ATTR_SERVICE_NAME]: 'fraud-detector',
    [ATTR_SERVICE_VERSION]: process.env.APP_VERSION || 'v2',
    'deployment.environment': process.env.ENV || 'production',
  }),
  traceExporter: new OTLPTraceExporter({
    // Envoyer les traces au Tempo via OTLP gRPC (port 4317)
    url: 'http://tempo.monitoring.svc.cluster.local:4317',
  }),
  instrumentations: [getNodeAutoInstrumentations({
    // Désactiver l'auto-instrumentation FS (trop bruyant)
    '@opentelemetry/instrumentation-fs': { enabled: false },
  })],
});

sdk.start();
console.log('[Tracing] OpenTelemetry initialisé — export vers Tempo');

module.exports = sdk;
