-- ═══════════════════════════════════════════════════════════════════════
-- FRC recharge reconciliation runbook (P0 outcome handling)
-- No DDL: parked rows reuse existing CHECK-constrained values.
--
-- A row is parked (never retried, never polled) when Pyro may already have executed
-- the recharge:
--   * timeout / transport error / unparseable response after the request was sent
--   * 5018 Duplicate clientTxnId (an earlier attempt registered at Pyro)
--   * 5016 same-subscriber block on a retry (earlier attempt likely succeeded)
--   * 2000 without status SUCCESS
-- Encoding:  push_flag = 'F'  AND final_status IS NULL  AND push_remarks LIKE 'RECONCILE:%'
-- (Real failures have final_status = 'FAILED'.)
-- ═══════════════════════════════════════════════════════════════════════

-- 1. Rows waiting for reconciliation ---------------------------------------
SELECT reqid, client_txn_id, pyro_trans_id, gsmno, frcamt, vendormsisdn,
       pyro_final_statuscode, push_remarks, last_error_msg, updated_ts
FROM   public.frc_pyro_request_data
WHERE  push_flag = 'F'
  AND  final_status IS NULL
  AND  push_remarks LIKE 'RECONCILE:%'
ORDER  BY updated_ts DESC;

-- Attempt history for one row
-- SELECT api_stage, attempt_no, pyro_status_code, pyro_status_text, pyro_txn_id,
--        error_class, error_detail, call_started_at
-- FROM   public.frc_txn_log WHERE frc_reqid = <reqid> ORDER BY call_started_at;

-- 2. Closing a row after checking with Pyro (by clientTxnId) -----------------
-- 2a. Recharge DID happen:
-- UPDATE public.frc_pyro_request_data
-- SET    push_flag = 'Y', final_status = 'SUCCESS', pyro_status = 'SUC',
--        pyro_trans_id = COALESCE(pyro_trans_id, <pyro txn id>),
--        completed_at = CURRENT_TIMESTAMP,
--        push_remarks = 'Manual reconciliation: recharge confirmed by Pyro',
--        updated_ts = CURRENT_TIMESTAMP
-- WHERE  reqid = <reqid> AND push_flag = 'F' AND final_status IS NULL;
--
-- 2b. Recharge did NOT happen:
--     Do not re-queue with the same clientTxnId; Pyro will answer 5018 again.
-- UPDATE public.frc_pyro_request_data
-- SET    final_status = 'FAILED', pyro_status = 'FAL', completed_at = CURRENT_TIMESTAMP,
--        push_remarks = 'Manual reconciliation: not executed by Pyro',
--        updated_ts = CURRENT_TIMESTAMP
-- WHERE  reqid = <reqid> AND push_flag = 'F' AND final_status IS NULL;
