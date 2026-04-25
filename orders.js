/**
 * Database: shop
 * collection: orders
 * Endpoints:
 *   GET /orders/stats 
 *   GET /orders/top-products 
 */
const express = require('express');
const { MongoClient } = require('mongodb');

const app = express();
const PORT = 3001;
const MONGO_URI = 'mongodb://localhost:27017';
const DB_NAME = 'shop';

let db;

MongoClient.connect(MONGO_URI)
  .then(client => {
    db = client.db(DB_NAME);
    console.log(`Connected to MongoDB – database: "${DB_NAME}"`);
  })
  .catch(err => {
    console.error('MongoDB connection error:', err);
    process.exit(1);
  });

/**
 *   status    – order status: "completed" | "pending" | "cancelled"  (необяз)
 *   minDate   – start date (необяз)
 *   maxDate   – end date (необяз)
 *   format    – "json" | "xml"  (default: json)
 *
 * Пример:
 *   orders/stats?status=completed&minDate=2024-01-01&format=xml
 */
app.get('/orders/stats', async (req, res) => {
  try {
    const { status, minDate, maxDate, format = 'json' } = req.query; // получила параметры
    const matchFilter = {};

    if (status) {
      matchFilter.status = status;
    }

    if (minDate || maxDate) {
      matchFilter.orderDate = {};
      if (minDate) matchFilter.orderDate.$gte = new Date(minDate);
      if (maxDate) matchFilter.orderDate.$lte = new Date(maxDate);
    }

    // закинула параметры в matchfilter

    const groupStage = {
      _id: '$category',              // сгруппировала по категории
      totalOrders:  { $sum: 1 },     // суммирую по 1 за каждый заказ, т.е. считаю количество
      totalRevenue: { $sum: '$totalPrice' },   // суммирую цены всех заказов
      avgOrderValue:{ $avg: '$totalPrice' },   // среднее по ценам всех
      maxOrder:     { $max: '$totalPrice' },   // самый дорогой
    };

    const sortStage = { totalRevenue: -1 }; // отсортировала по сумме всех

    const projectStage = {
      _id: 0,                                          // прячу ID
      category:      '$_id',                           // переименовываю ID в категорию
      totalOrders:   1,                                // как есть
      totalRevenue:  { $round: ['$totalRevenue', 2] }, // округления
      avgOrderValue: { $round: ['$avgOrderValue', 2] },
      maxOrder:      { $round: ['$maxOrder', 2] },
    };

    const pipeline = [ // собрала всё в пайплайн
      { $match:   matchFilter },
      { $group:   groupStage  },
      { $sort:    sortStage   },
      { $project: projectStage },
    ];

    const results = await db 
      .collection('orders')
      .aggregate(pipeline) 
      .toArray();

    if (format === 'xml') {
      const xml = buildXml('salesStats', 'category', results);
      res.set('Content-Type', 'application/xml');
      return res.send(xml);
    }

    res.json({
      pipeline: {
        stages: ['$match', '$group', '$sort', '$project'],
        appliedFilters: { status: status ?? 'all', minDate: minDate ?? null, maxDate: maxDate ?? null }
      },
      totalCategories: results.length,
      stats: results
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/**
 *   category  – filter by product category  (optional)
 *   limit     – how many top products to show (default: 5)
 *   format    – "json" | "xml"  (default: json)
 * Пример:
 *   /orders/top-products?category=electronics&limit=3
 */
app.get('/orders/top-products', async (req, res) => {
  try {
    const { category, limit = '5', format = 'json' } = req.query;
    const topN = parseInt(limit, 10);

    const matchFilter = {};
    if (category) {
      matchFilter.category = { $regex: category, $options: 'i' };
    }

    const groupStage = {
      _id:           '$productName',
      totalSold:     { $sum: '$quantity' },
      totalRevenue:  { $sum: '$totalPrice' },
      timesOrdered:  { $sum: 1 },
    };

    const sortStage = { totalSold: -1 }; //

    const projectStage = {
      _id:          0, // убираю id
      product:      '$_id', 
      totalSold:    1,
      timesOrdered: 1,
      totalRevenue: { $round: ['$totalRevenue', 2] },
    };

    const pipeline = [
      { $match:   matchFilter },
      { $group:   groupStage  },
      { $sort:    sortStage   },
      { $limit:   topN        },   //  только топ N элментов
      { $project: projectStage },
    ];

    const results = await db
      .collection('orders')
      .aggregate(pipeline)
      .toArray();

    if (format === 'xml') {
      const xml = buildXml('topProducts', 'product', results);
      res.set('Content-Type', 'application/xml');
      return res.send(xml);
    }

    res.json({
      pipeline: {
        stages: ['$match', '$group', '$sort', '$limit', '$project'],
        category: category ?? 'all',
        limit: topN
      },
      results
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// мучаюсь с XML
function escapeXml(val) {
  return String(val ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function buildXml(rootTag, itemTag, items) {
  const rows = items.map(item => {
    const fields = Object.entries(item)
      .map(([k, v]) => `    <${k}>${escapeXml(v)}</${k}>`)
      .join('\n');
    return `  <${itemTag}>\n${fields}\n  </${itemTag}>`;
  }).join('\n');

  return `<?xml version="1.0" encoding="UTF-8"?>\n<${rootTag}>\n${rows}\n</${rootTag}>`;
}

// Заполнение
app.get('/seed', async (req, res) => {
  try {
    await db.collection('orders').drop().catch(() => {});
    await db.collection('orders').insertMany([
      { productName: 'Laptop Pro',      category: 'electronics', quantity: 2, totalPrice: 2599.98, status: 'completed', orderDate: new Date('2024-01-15') },
      { productName: 'Wireless Mouse',  category: 'electronics', quantity: 5, totalPrice:  149.95, status: 'completed', orderDate: new Date('2024-02-10') },
      { productName: 'Wireless Mouse',  category: 'electronics', quantity: 3, totalPrice:   89.97, status: 'completed', orderDate: new Date('2024-03-05') },
      { productName: 'JavaScript Book', category: 'books',       quantity: 4, totalPrice:  159.96, status: 'completed', orderDate: new Date('2024-01-20') },
      { productName: 'Node.js Guide',   category: 'books',       quantity: 2, totalPrice:   69.98, status: 'pending',   orderDate: new Date('2024-02-28') },
      { productName: 'Running Shoes',   category: 'clothing',    quantity: 1, totalPrice:   89.99, status: 'completed', orderDate: new Date('2024-03-15') },
      { productName: 'Winter Jacket',   category: 'clothing',    quantity: 2, totalPrice:  299.98, status: 'completed', orderDate: new Date('2024-04-01') },
      { productName: 'Coffee Maker',    category: 'appliances',  quantity: 3, totalPrice:  179.97, status: 'cancelled', orderDate: new Date('2024-04-10') },
      { productName: 'Laptop Pro',      category: 'electronics', quantity: 1, totalPrice: 1299.99, status: 'completed', orderDate: new Date('2024-05-01') },
      { productName: 'JavaScript Book', category: 'books',       quantity: 6, totalPrice:  239.94, status: 'completed', orderDate: new Date('2024-05-20') },
    ]);
    res.json({ message: 'Orders seeded successfully.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () =>
  console.log(`File 1 server running → http://localhost:${PORT}`)
);
