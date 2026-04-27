const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const redis = require("redis");

const app = express();
app.use(cors({ origin: "http://localhost:3000" }));
app.use(express.json());


// ==========================
// ENV CONFIG
// ==========================
const DB_HOST = process.env.DB_HOST || "localhost";
const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";


// ==========================
// MYSQL POOL
// ==========================
const pool = mysql.createPool({
  host: DB_HOST,
  user: "root",
  password: "",
  database: "medsync",
  waitForConnections: true,
  connectionLimit: 10
});


// ==========================
// REDIS SETUP (SAFE)
// ==========================
let redisClient = null;

(async () => {
  try {
    redisClient = redis.createClient({
      socket: {
        host: REDIS_HOST,
        port: 6379
      }
    });

    redisClient.on("error", (err) =>
      console.error("Redis Error:", err.message)
    );

    await redisClient.connect();
    console.log("Redis Connected ✅");

  } catch (err) {
    console.log("Redis not available, running without cache");
    redisClient = null;
  }
})();


// ==========================
// MOCK KAFKA
// ==========================
function publishToKafka(event) {
  console.log("Kafka Event:", event);
}


// ==========================
// UUID VALIDATION
// ==========================
function isValidUUID(uuid) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid);
}


// ==========================
// HOME
// ==========================
app.get("/", (req, res) => {
  res.send("🚀 MedSync API Running");
});


// ==========================
// POST /vitals
// ==========================
app.post("/vitals", async (req, res) => {
  const { patient_id, heart_rate, spo2, systolic, diastolic, recorded_at } = req.body;

  // -------- VALIDATION --------
  if (!patient_id || !isValidUUID(patient_id))
    return res.status(400).json({ error: "Invalid patient_id" });

  if (!Number.isInteger(heart_rate) || heart_rate <= 0)
    return res.status(400).json({ error: "Invalid heart_rate" });

  if (!Number.isInteger(spo2) || spo2 < 0 || spo2 > 100)
    return res.status(400).json({ error: "Invalid spo2" });

  if (!Number.isInteger(systolic) || systolic <= 0)
    return res.status(400).json({ error: "Invalid systolic" });

  if (!Number.isInteger(diastolic) || diastolic <= 0)
    return res.status(400).json({ error: "Invalid diastolic" });

  if (!recorded_at || isNaN(new Date(recorded_at).getTime()))
    return res.status(400).json({ error: "Invalid recorded_at" });


  // -------- ALERT LOGIC --------
  let alerts = [];

  if (heart_rate < 40) alerts.push({ type: "heart_rate_low", value: heart_rate });
  if (heart_rate > 150) alerts.push({ type: "heart_rate_high", value: heart_rate });
  if (spo2 < 90) alerts.push({ type: "spo2_low", value: spo2 });
  if (systolic > 180) alerts.push({ type: "systolic_high", value: systolic });
  if (systolic < 60) alerts.push({ type: "systolic_low", value: systolic });


  let conn;

  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    // Insert vitals
    await conn.query(
      `INSERT INTO vitals 
       (patient_id, heart_rate, spo2, systolic, diastolic, recorded_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [patient_id, heart_rate, spo2, systolic, diastolic, recorded_at]
    );

    // Insert alerts + Kafka
    for (let alert of alerts) {
      await conn.query(
        `INSERT INTO alerts (patient_id, alert_type, reading_value)
         VALUES (?, ?, ?)`,
        [patient_id, alert.type, alert.value]
      );

      publishToKafka({
        patient_id,
        alert_type: alert.type,
        value: alert.value
      });
    }

    await conn.commit();

    // Cache invalidate
    if (redisClient && redisClient.isOpen) {
      await redisClient.del(`vitals:${patient_id}`);
    }

    res.json({
      success: true,
      message: "Vitals recorded successfully"
    });

  } catch (err) {
    if (conn) await conn.rollback();
    console.error(err);

    res.status(500).json({
      error: "Internal server error"
    });

  } finally {
    if (conn) conn.release();
  }
});


// ==========================
// GET /patients/:id/vitals/latest
// ==========================
app.get("/patients/:patient_id/vitals/latest", async (req, res) => {
  const { patient_id } = req.params;

  if (!isValidUUID(patient_id)) {
    return res.status(400).json({ error: "Invalid patient_id" });
  }

  const cacheKey = `vitals:${patient_id}`;

  try {
    // -------- CACHE CHECK --------
    if (redisClient && redisClient.isOpen) {
      const cached = await redisClient.get(cacheKey);

      if (cached) {
        return res.json({
          success: true,
          data: JSON.parse(cached),
          cached: true
        });
      }
    }

    // -------- DB QUERY --------
    const [rows] = await pool.query(
      `SELECT id, patient_id, heart_rate, spo2, systolic, diastolic, recorded_at
       FROM vitals
       WHERE patient_id = ?
       ORDER BY recorded_at DESC
       LIMIT 10`,
      [patient_id]
    );

    // -------- CACHE STORE --------
    if (redisClient && redisClient.isOpen) {
      await redisClient.setEx(cacheKey, 60, JSON.stringify(rows));
    }

    res.json({
      success: true,
      data: rows,
      cached: false
    });

  } catch (err) {
    console.error(err);

    res.status(500).json({
      error: "Internal server error"
    });
  }
});


// ==========================
// START SERVER
// ==========================
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});