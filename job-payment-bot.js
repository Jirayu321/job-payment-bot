// job-payment-bot.js
require("dotenv").config();
const sql = require("mssql");
const axios = require("axios");

// ====== CONFIG ======
// console.log("ENV:", {
//   DB_HOST: process.env.DB_HOST,
//   DB_NAME: process.env.DB_NAME,
//   DB_USER: process.env.DB_USER,
//   DB_PASS_LEN: process.env.DB_PASS?.length,
// });

const DB_CONFIG = {
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  server: process.env.DB_HOST,
  database: process.env.DB_NAME,
  port: Number(process.env.DB_PORT || 1433),
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
};

const BACKEND_BASE_URL =
  process.env.BACKEND_BASE_URL ||
  "https://pbpos-backend.appsystemyou.com";

const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 5000);
const MAX_ROWS_PER_CYCLE = Number(process.env.MAX_ROWS_PER_CYCLE || 20);

// 🔹 UPDATE: Mark expired payments
async function markPaymentExpired(hisPaymentId, reason) {
  const pool = await sql.connect(DB_CONFIG);

  await pool
    .request()
    .input("HisPaymentId", sql.Int, hisPaymentId)
    .input("StatusDesc", sql.VarChar(255), reason || "QR is expired")
    .query(`
      UPDATE dbo.His_Payment
      SET
        PaymentStatus      = 'EXPIRED',
        StatusDesc         = @StatusDesc,
        BankResultDateTime = GETDATE(),
        UpdatedDateTime    = GETDATE(),
        UpdatedBy          = 'SYSTEM'
      WHERE HisPaymentId = @HisPaymentId
        AND PaymentStatus = 'PENDING';
    `);

  console.log(`💀 Marked HisPaymentId=${hisPaymentId} as EXPIRED (reason="${reason}")`);
}

// ====== MAIN LOGIC ======
async function getPendingJobPayments() {
  const pool = await sql.connect(DB_CONFIG);

  const result = await pool
    .request()
    .input("MaxRows", sql.Int, MAX_ROWS_PER_CYCLE)
    .query(`
      SELECT TOP (@MaxRows)
        HisPaymentId,
        ChargeId,
        PaymentStatus,
        SourceType,
        Amount,
        CreatedDateTime
      FROM dbo.His_Payment
      WHERE
        SourceType = 'JOB'
        AND PaymentStatus = 'PENDING'
        AND ChargeId IS NOT NULL
        AND CreatedDateTime >= DATEADD(MINUTE, -60, GETDATE())
      ORDER BY HisPaymentId DESC;
    `);

  return result.recordset;
}

async function hitBackendQrTxJob(chargeId) {
  const url = `${BACKEND_BASE_URL}/payment/qr-tx-job/${encodeURIComponent(
    chargeId
  )}`;

  console.log(`🚀 CALL: ${url}`);

  const resp = await axios.get(url, {
    timeout: 10000,
  });

  const status =
    resp.data?.status ||
    resp.data?.kbank?.status ||
    resp.data?.payment_status ||
    resp.status;

  console.log(`✔ ChargeId=${chargeId}, Status=${status}`);

  return resp.data;
}

async function processCycle() {
  try {
    console.log("\n===== BOT CYCLE START =====");

    const rows = await getPendingJobPayments();
    if (!rows.length) {
      console.log("No pending job payments within 60 minutes.");
      console.log("===== BOT CYCLE END (EMPTY) =====");
      return;
    }

    console.log(`Found ${rows.length} pending within 60 mins.`);

    for (const row of rows) {
      const { HisPaymentId, ChargeId } = row;

      console.log(
        `📌 PROCESS HisPaymentId=${HisPaymentId}, ChargeId=${ChargeId}`
      );

      try {
        await hitBackendQrTxJob(ChargeId);
      } catch (err) {
        const data = err.response?.data;
        console.error(
          `❌ Backend call failed for ChargeId=${ChargeId}:`,
          data || err.message || err
        );

        const code =
          data?.message?.code || data?.code || data?.error_code;

        // 🔻 ถ้า QR หมดอายุ → mark expired
        if (code === "transaction_expired") {
          const msg =
            data?.message?.message ||
            data?.message ||
            "QR is expired and cannot be used.";
          await markPaymentExpired(HisPaymentId, msg);
        }
      }
    }

    console.log("===== BOT CYCLE END =====");
  } catch (err) {
    console.error("💀 BOT ERROR:", err);
  }
}

// ====== START BOT LOOP ======
async function startLoop() {
  try {
    await sql.connect(DB_CONFIG);
    console.log("💾 DB Connected");
  } catch (err) {
    console.error("DB connect failed:", err);
    process.exit(1);
  }

  await processCycle();
  setInterval(processCycle, POLL_INTERVAL_MS);
}

startLoop().catch((err) => {
  console.error("Bot crashed:", err);
  process.exit(1);
});
