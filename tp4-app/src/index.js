const express = require('express');
const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8080;

app.get('/health', (req, res) => res.json({ status: 'ok', service: 'finsecure-api' }));

app.get('/merchants', async (req, res) => {
  try {
    const { withCache } = require('./cache-service');
    const merchants = await withCache(
      'merchants:all',
      async () => {
        await new Promise(r => setTimeout(r, 200));
        return [
          { id: 'MERCH-1', name: 'BoutiqueA', category: 'retail' },
          { id: 'MERCH-2', name: 'MarketplaceB', category: 'marketplace' },
          { id: 'MERCH-3', name: 'EcommerceC', category: 'fashion' },
        ];
      },
      3600
    );
    res.json({ source: 'cache_or_db', count: merchants.length, data: merchants });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/merchants', async (req, res) => {
  const { invalidateCache } = require('./cache-service');
  await invalidateCache('merchants:all');
  res.status(201).json({ message: 'Marchand créé, cache invalidé' });
});

app.listen(PORT, () => console.log(`FinSecure API listening on port ${PORT}`));
