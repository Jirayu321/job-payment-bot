// job-payment-bot.js (patched)

require("dotenv").config();
const sql = require("mssql");
const axios = require("axios");

// ====== CONFIG ======
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
  process.env.BACKEND_BASE_URL || "https://pbpos-backend.appsystemyou.com";

const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 5000);
const MAX_ROWS_PER_CYCLE = Number(process.env.MAX_ROWS_PER_CYCLE || 20);

const CHARGE_DEDUPE_TTL_MS = Number(process.env.CHARGE_DEDUPE_TTL_MS || 120000); 

const MIN_RECHECK_SECONDS = Number(process.env.MIN_RECHECK_SECONDS || 30);

const PROCESSING_STALE_SECONDS = Number(
  process.env.PROCESSING_STALE_SECONDS || 180
); // 3 นาที

const chargeLastCalledAt = new Map();

function nowMs() {
  return Date.now();
}

function isWithinTtl(lastAt, ttlMs) {
  return typeof lastAt === "number" && nowMs() - lastAt < ttlMs;
}

function normStatus(s) {
  return String(s || "")
    .trim()
    .toUpperCase();
}

function interpretBackendStatus(respData, httpStatus) {

  const raw =
    respData?.status ||
    respData?.kbank?.status ||
    respData?.payment_status ||
    httpStatus;

  const status = normStatus(raw);

  const isPaid =
    status === "SUCCESS" ||
    status === "PAID" ||
    status === "COMPLETED" ||
    status === "DONE" ||
    status === "APPROVED";

  const isPending =
    status === "PENDING" ||
    status === "PROCESSING" ||
    status === "IN_PROGRESS" ||
    status === "CREATED";

  const isExpired = status === "EXPIRED";

  return { status, isPaid, isPending, isExpired };
}


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
        AND PaymentStatus IN ('PENDING','PROCESSING');
    `);

  console.log(
    `💀 Marked HisPaymentId=${hisPaymentId} as EXPIRED (reason="${reason}")`
  );
}

async function markPaymentSuccess(hisPaymentId, note) {
  const pool = await sql.connect(DB_CONFIG);

  await pool
    .request()
    .input("HisPaymentId", sql.Int, hisPaymentId)
    .input("StatusDesc", sql.VarChar(255), note || "Payment success")
    .query(`
      UPDATE dbo.His_Payment
      SET
        PaymentStatus      = 'SUCCESS',
        StatusDesc         = @StatusDesc,
        BankResultDateTime = GETDATE(),
        UpdatedDateTime    = GETDATE(),
        UpdatedBy          = 'JOB_BOT'
      WHERE HisPaymentId = @HisPaymentId
        AND PaymentStatus IN ('PENDING','PROCESSING');
    `);

  console.log(`✅ Marked HisPaymentId=${hisPaymentId} as SUCCESS`);
}

async function revertToPending(hisPaymentId, reason) {
  const pool = await sql.connect(DB_CONFIG);

  await pool
    .request()
    .input("HisPaymentId", sql.Int, hisPaymentId)
    .input("StatusDesc", sql.VarChar(255), reason || "Still pending")
    .query(`
      UPDATE dbo.His_Payment
      SET
        PaymentStatus   = 'PENDING',
        StatusDesc      = @StatusDesc,
        UpdatedDateTime = GETDATE(),
        UpdatedBy       = 'JOB_BOT'
      WHERE HisPaymentId = @HisPaymentId
        AND PaymentStatus = 'PROCESSING';
    `);

  console.log(`↩️ Reverted HisPaymentId=${hisPaymentId} back to PENDING`);
}

async function claimPaymentRow(hisPaymentId) {
  const pool = await sql.connect(DB_CONFIG);

  const result = await pool
    .request()
    .input("HisPaymentId", sql.Int, hisPaymentId)
    .input("StaleSeconds", sql.Int, PROCESSING_STALE_SECONDS)
    .query(`
      UPDATE dbo.His_Payment
      SET
        PaymentStatus   = 'PROCESSING',
        UpdatedDateTime = GETDATE(),
        UpdatedBy       = 'JOB_BOT'
      WHERE HisPaymentId = @HisPaymentId
        AND (
          PaymentStatus = 'PENDING'
          OR (
            PaymentStatus = 'PROCESSING'
            AND UpdatedDateTime < DATEADD(SECOND, -@StaleSeconds, GETDATE())
          )
        );

      SELECT @@ROWCOUNT AS affected;
    `);

  return result.recordset?.[0]?.affected === 1;
}

// ====== MAIN LOGIC ======

async function getPendingJobPayments() {
  const pool = await sql.connect(DB_CONFIG);

  const result = await pool
    .request()
    .input("MaxRows", sql.Int, MAX_ROWS_PER_CYCLE)
    .input("MinRecheckSeconds", sql.Int, MIN_RECHECK_SECONDS)
    .query(`
      SELECT TOP (@MaxRows)
        HisPaymentId,
        ChargeId,
        PaymentStatus,
        SourceType,
        Amount,
        CreatedDateTime,
        UpdatedDateTime
      FROM dbo.His_Payment
      WHERE
        SourceType = 'JOB'
        AND PaymentStatus IN ('PENDING','PROCESSING')
        AND ChargeId IS NOT NULL
        AND CreatedDateTime >= DATEADD(MINUTE, -60, GETDATE())
        AND (
          UpdatedDateTime IS NULL
          OR UpdatedDateTime < DATEADD(SECOND, -@MinRecheckSeconds, GETDATE())
        )
      ORDER BY HisPaymentId DESC;
    `);

  return result.recordset;
}

async function hitBackendQrTxJob(chargeId) {
  const url = `${BACKEND_BASE_URL}/payment/qr-tx-job/${encodeURIComponent(
    chargeId
  )}`;

  console.log(`🚀 CALL: ${url}`);

  const resp = await axios.get(url, { timeout: 10000 });

  const { status } = interpretBackendStatus(resp.data, resp.status);
  console.log(`✔ ChargeId=${chargeId}, Status=${status}`);

  return resp.data;
}

async function processOneRow(row) {
  const { HisPaymentId, ChargeId } = row;

  // 1) กันยิงซ้ำด้วย ChargeId TTL (ลด duplicate confirm)
  const last = chargeLastCalledAt.get(ChargeId);
  if (isWithinTtl(last, CHARGE_DEDUPE_TTL_MS)) {
    console.log(
      `⏭️ SKIP (dedupe TTL) HisPaymentId=${HisPaymentId}, ChargeId=${ChargeId}`
    );
    return;
  }

  const claimed = await claimPaymentRow(HisPaymentId);
  if (!claimed) {
    console.log(
      `⏭️ SKIP (not claimed) HisPaymentId=${HisPaymentId}, ChargeId=${ChargeId}`
    );
    return;
  }
  chargeLastCalledAt.set(ChargeId, nowMs());

  console.log(`📌 PROCESS HisPaymentId=${HisPaymentId}, ChargeId=${ChargeId}`);

  try {
    const data = await hitBackendQrTxJob(ChargeId);
    const { status, isPaid, isPending, isExpired } = interpretBackendStatus(
      data,
      200
    );

    if (isPaid) {
      await markPaymentSuccess(HisPaymentId, `Backend status=${status}`);
      return;
    }

    if (isExpired) {
      await markPaymentExpired(HisPaymentId, `Backend status=${status}`);
      return;
    }

    if (isPending) {
      await revertToPending(HisPaymentId, `Backend status=${status}`);
      return;
    }

    await revertToPending(HisPaymentId, `Unknown backend status=${status}`);
  } catch (err) {
    const data = err.response?.data;
    const code = data?.message?.code || data?.code || data?.error_code;

    console.error(
      `❌ Backend call failed for ChargeId=${ChargeId}:`,
      data || err.message || err
    );

    if (code === "transaction_expired") {
      const msg =
        data?.message?.message ||
        data?.message ||
        "QR is expired and cannot be used.";
      await markPaymentExpired(HisPaymentId, msg);
      return;
    }

    await revertToPending(
      HisPaymentId,
      `Backend error: ${code || err.message || "unknown"}`
    );
  }
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

    console.log(`Found ${rows.length} candidate rows.`);

    for (const row of rows) {
      await processOneRow(row);
    }

    console.log("===== BOT CYCLE END =====");
  } catch (err) {
    console.error("💀 BOT ERROR:", err);
  }
}


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
