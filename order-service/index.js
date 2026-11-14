import express from 'express';
import axios from 'axios';
import db from './db.js';
import { connectToBroker, publishMessage } from './broker.js';

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch(err => console.error('Broker init error', err));

// Create order
app.post('/', async (req, res) => {
  // TODO: Implement order creation with the following steps:
  // 1. Validate request body:
  //    - Check productId exists
  //    - Check quantity is positive
  try {
    const { productId, quantity } = req.body;
    if(!Number.isInteger(productId) || Number(productId) <= 0) {
      return res.status(400).json({ error: 'productId must be a positive integer' });
    }
    if(!Number.isInteger(quantity) || Number(quantity) <= 0) {
      return res.status(400).json({ error: 'quantity must be a positive integer' });
    }
  // 2. Call product service to verify product exists:
  //    - Use axios to GET product details
  //    - Handle timeouts and errors
  const response = await axios.get(
    `${process.env.PRODUCT_SERVICE_URL}/${Number(productId)}`, 
    { timeout: 5000 });
  const product = response.data;
  // 3. Insert order into database:
  //    - Add to orders table with PENDING status
  const result = await db.query(
    `INSERT INTO orders (product_id, quantity, status)
    VALUES ($1, $2, $3) 
    RETURNING *`,
    [productId, Number(quantity), 'PENDING']
  );
  const order = result.rows[0];
  // 4. Publish order.created event to message broker:
  //    - Include order id, product details, quantity
  // 5. Return success response with order details
  await publishMessage('order.created', {
    event: 'ORDER.CREATED',
    orderId: order.id,
    productId: order.product_id,
    productTitle: product.title,
    quantity: Number(quantity)
  });
  res.status(201).json({
    message: 'Order created successfully',
    order
  });
  } catch (err) {
    console.error('Create order error:', err);
    if(axios.isAxiosError(err) && err.response?.status === 404) {
      return res.status(404).json({ error: 'Product not found' });
    }
    if(err instanceof Error && err.message === 'RabbitMQ channel is not ready') {
      res.status(503).json({ error: 'Message broker is not ready' });
    }
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// List orders
app.get('/', async (_req, res) => {
  const r = await db.query('SELECT * FROM orders ORDER BY id DESC');
  res.json(r.rows);
});

// Get order by id
app.get('/:id', async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query('SELECT * FROM orders WHERE id = $1', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'Order not found' });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));
