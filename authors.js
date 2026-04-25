/**
 * Database: blog
 * Collections: articles, authors
 *
 * Endpoints:
 *   GET /blog/articles-with-authors 
 *   GET /blog/author-stats 
 */

const express = require('express');
const { MongoClient } = require('mongodb');

const app = express();
const PORT = 3002;
const MONGO_URI = 'mongodb://localhost:27017';
const DB_NAME = 'blog';

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
 * Query params:
 *   category  – filter by article category  (необяз)
 *   minViews  – minimum number of views     (необяз, default: 0)
 *   format    – "json" | "xml"              (default: json)
 *
 * Пример:
 *   /blog/articles-with-authors?category=tech&minViews=100
 */
app.get('/blog/articles-with-authors', async (req, res) => {
  try {
    const { category, minViews = '0', format = 'json' } = req.query;

    const matchFilter = {
      views: { $gte: parseInt(minViews, 10) }
    };
    if (category) {
      matchFilter.category = { $regex: category, $options: 'i' };
    }

    // Как в SQL JOIN 
    // "from" это c какой табл
    // "localField" поле из этой табл
    // "foreignField" поле в другой табл
    // "as" имя для результирующего массива
    const lookupStage = {
      from:         'authors',  
      localField:   'authorId',
      foreignField: '_id', 
      as:           'authorInfo',
    };

    // $lookup returns authorInfo as an ARRAY 

    const projectStage = {
      _id:         0,
      title:       1,
      category:    1,
      views:       1,
      publishedAt: 1,
      //вытаскиваю поля из массива
      authorName:  { $arrayElemAt: ['$authorInfo.name',    0] },
      authorEmail: { $arrayElemAt: ['$authorInfo.email',   0] },
      authorCity:  { $arrayElemAt: ['$authorInfo.city',    0] },
    };

    const sortStage = { views: -1 };

    const pipeline = [
      { $match:   matchFilter  },
      { $lookup:  lookupStage  },
      { $project: projectStage },
      { $sort:    sortStage    },
    ];

    const results = await db
      .collection('articles')
      .aggregate(pipeline)
      .toArray();

    if (format === 'xml') {
      const xml = buildXml('articlesWithAuthors', 'article', results);
      res.set('Content-Type', 'application/xml');
      return res.send(xml);
    }

    res.json({
      pipeline: {
        stages: ['$match', '$lookup', '$project', '$sort'],
        joinedCollections: ['articles', 'authors'],
        filters: { category: category ?? 'all', minViews }
      },
      totalFound: results.length,
      articles: results
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/**
 *   category   – filter articles by category  (необяз)
 *   sortBy     – "views" | "articles"         (default: views)
 *   format     – "json" | "xml"               (default: json)
 *
 * Пример:
 *   /blog/author-stats?sortBy=articles
 */
app.get('/blog/author-stats', async (req, res) => {
  try {
    const { category, sortBy = 'views', format = 'json' } = req.query;

    const matchFilter = {};
    if (category) {
      matchFilter.category = { $regex: category, $options: 'i' };
    }

    const groupStage = {
      _id:           '$authorId',
      totalArticles: { $sum: 1 },
      totalViews:    { $sum: '$views' },
      avgViews:      { $avg: '$views' },
      categories:    { $addToSet: '$category' },
    };

    const lookupStage = {
      from:         'authors',
      localField:   '_id',       // after $group, _id is the authorId
      foreignField: '_id',
      as:           'authorInfo',
    };

    const projectStage = {
      _id:           0,
      authorName:    { $arrayElemAt: ['$authorInfo.name',  0] },
      authorEmail:   { $arrayElemAt: ['$authorInfo.email', 0] },
      totalArticles: 1,
      totalViews:    1,
      avgViews:      { $round: ['$avgViews', 0] },
      categories:    1,
    };

    const sortField = sortBy === 'articles' ? 'totalArticles' : 'totalViews';
    const sortStage = { [sortField]: -1 };

    const pipeline = [
      { $match:   matchFilter  },
      { $group:   groupStage   },
      { $lookup:  lookupStage  },
      { $project: projectStage },
      { $sort:    sortStage    },
    ];

    const results = await db
      .collection('articles')
      .aggregate(pipeline)
      .toArray();

    if (format === 'xml') {
      const flat = results.map(r => ({ ...r, categories: r.categories.join(', ') }));
      const xml = buildXml('authorStats', 'author', flat);
      res.set('Content-Type', 'application/xml');
      return res.send(xml);
    }

    res.json({
      pipeline: {
        stages: ['$match', '$group', '$lookup', '$project', '$sort'],
        sortBy: sortField,
        categoryFilter: category ?? 'all'
      },
      totalAuthors: results.length,
      authors: results
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// XML формирую
function escapeXml(val) {
  return String(val ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function buildXml(rootTag, itemTag, items) {
  const rows = items.map(item => {
    const fields = Object.entries(item)
      .map(([k, v]) => `    <${k}>${escapeXml(Array.isArray(v) ? v.join(', ') : v)}</${k}>`)
      .join('\n');
    return `  <${itemTag}>\n${fields}\n  </${itemTag}>`;
  }).join('\n');
  return `<?xml version="1.0" encoding="UTF-8"?>\n<${rootTag}>\n${rows}\n</${rootTag}>`;
}

// Заполнения таблиц
app.get('/seed', async (req, res) => {
  try {
    const { ObjectId } = require('mongodb');

    await db.collection('authors').drop().catch(() => {});
    await db.collection('articles').drop().catch(() => {});

    // Insert authors first and capture their IDs
    const authorDocs = [
      { name: 'Alice Anderson', email: 'alice@blog.com', city: 'Amsterdam' },
      { name: 'Bob Brennan',    email: 'bob@blog.com',   city: 'Berlin'    },
      { name: 'Carol Chan',     email: 'carol@blog.com', city: 'London'    },
    ];
    const authorsResult = await db.collection('authors').insertMany(authorDocs);
    const [aliceId, bobId, carolId] = Object.values(authorsResult.insertedIds);

    // Insert articles referencing author IDs
    await db.collection('articles').insertMany([
      { title: 'Getting Started with Node.js',    category: 'tech',   views: 1520, authorId: aliceId, publishedAt: '2024-01-10' },
      { title: 'MongoDB Aggregation Pipelines',   category: 'tech',   views:  980, authorId: aliceId, publishedAt: '2024-02-15' },
      { title: 'JavaScript Async/Await',          category: 'tech',   views: 2100, authorId: bobId,   publishedAt: '2024-03-05' },
      { title: 'REST API Best Practices',         category: 'tech',   views:  750, authorId: bobId,   publishedAt: '2024-03-20' },
      { title: 'Healthy Morning Routines',        category: 'health', views:  430, authorId: carolId, publishedAt: '2024-04-01' },
      { title: 'Introduction to Docker',          category: 'tech',   views: 1200, authorId: carolId, publishedAt: '2024-04-12' },
      { title: 'Travel Guide: Amsterdam',         category: 'travel', views:  890, authorId: aliceId, publishedAt: '2024-05-01' },
      { title: 'Express.js Middleware Guide',     category: 'tech',   views:  670, authorId: bobId,   publishedAt: '2024-05-18' },
    ]);

    res.json({ message: 'Blog data seeded successfully.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () =>
  console.log(`File 2 server running → http://localhost:${PORT}`)
);
