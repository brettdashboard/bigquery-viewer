const express = require('express');
const cors = require('cors');
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3002;

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname)));

// In-memory storage for BigQuery client
let bigQueryClient = null;
let bigQueryCredentials = null;

// Check connection status
app.get('/api/bigquery/status', (req, res) => {
    res.json({ 
        connected: bigQueryClient !== null,
        projectId: bigQueryCredentials?.projectId || null
    });
});

// Connect to BigQuery
app.post('/api/bigquery/connect', async (req, res) => {
    const { projectId, clientEmail, privateKey } = req.body;

    if (!projectId || !clientEmail || !privateKey) {
        return res.status(400).json({ error: 'Missing required credentials' });
    }

    try {
        const formattedKey = privateKey.replace(/\\n/g, '\n');
        
        bigQueryClient = new BigQuery({
            projectId,
            credentials: {
                client_email: clientEmail,
                private_key: formattedKey
            }
        });

        // Test connection
        await bigQueryClient.getDatasets();
        
        bigQueryCredentials = { projectId, clientEmail };
        
        res.json({ success: true, message: 'Connected to BigQuery' });
    } catch (error) {
        bigQueryClient = null;
        bigQueryCredentials = null;
        res.status(401).json({ error: 'Authentication failed: ' + error.message });
    }
});

// Disconnect
app.post('/api/bigquery/disconnect', (req, res) => {
    bigQueryClient = null;
    bigQueryCredentials = null;
    res.json({ success: true });
});

// List datasets
app.get('/api/bigquery/datasets', async (req, res) => {
    if (!bigQueryClient) {
        return res.status(401).json({ error: 'Not connected to BigQuery' });
    }

    try {
        const [datasets] = await bigQueryClient.getDatasets();
        res.json({ 
            datasets: datasets.map(ds => ({ 
                id: ds.id,
                location: ds.metadata?.location 
            }))
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// List tables in dataset
app.get('/api/bigquery/tables/:datasetId', async (req, res) => {
    if (!bigQueryClient) {
        return res.status(401).json({ error: 'Not connected to BigQuery' });
    }

    try {
        const dataset = bigQueryClient.dataset(req.params.datasetId);
        const [tables] = await dataset.getTables();
        res.json({ 
            tables: tables.map(t => ({ 
                id: t.id,
                type: t.metadata?.type
            }))
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Get table data
app.get('/api/bigquery/data/:datasetId/:tableId', async (req, res) => {
    if (!bigQueryClient) {
        return res.status(401).json({ error: 'Not connected to BigQuery' });
    }

    const { datasetId, tableId } = req.params;
    const limit = parseInt(req.query.limit) || 100;
    const offset = parseInt(req.query.offset) || 0;

    try {
        const dataset = bigQueryClient.dataset(datasetId);
        const table = dataset.table(tableId);
        const [metadata] = await table.getMetadata();
        const schema = metadata.schema?.fields || [];

        const query = `SELECT * FROM \`${bigQueryCredentials.projectId}.${datasetId}.${tableId}\` LIMIT ${limit} OFFSET ${offset}`;
        const [rows] = await bigQueryClient.query(query);

        res.json({ rows, schema, total: metadata.numRows });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Run custom query
app.post('/api/bigquery/query', async (req, res) => {
    if (!bigQueryClient) {
        return res.status(401).json({ error: 'Not connected to BigQuery' });
    }

    const { query } = req.body;
    if (!query) {
        return res.status(400).json({ error: 'Query is required' });
    }

    try {
        const [rows] = await bigQueryClient.query(query);
        const schema = rows.length > 0 
            ? Object.keys(rows[0]).map(name => ({ name, type: typeof rows[0][name] }))
            : [];
        res.json({ rows, schema });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`\n${'='.repeat(50)}`);
    console.log(`BigQuery Data Viewer Server`);
    console.log(`${'='.repeat(50)}`);
    console.log(`Running on: http://localhost:${PORT}`);
    console.log(`${'='.repeat(50)}\n`);
});