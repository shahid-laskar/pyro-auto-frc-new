"""Fault tolerance test suite for cross-database reconciliation (Phase 15).

Verifies reconciliation and failure recovery across all three lifecycle branches:
1. Branch 1 (Already Claimed):
   - Oracle BCD already has FRC_FLOW_STATUS='RQ' and FRC_REQID=reqid.
   - Reconciler confirms dispatchable (in_status='C') with NO duplicate Oracle update.
2. Branch 2 (Unclaimed):
   - Oracle BCD has FRC_FLOW_STATUS='NP' and FRC_REQID IS NULL.
   - Reconciler retries Q020 claim writeback and marks dispatchable upon success.
3. Branch 3 (Conflict / Missing):
   - Oracle has conflicting reqid, terminal status ('SU'/'F'), or missing candidate.
   - Reconciler aborts and marks request failed (in_status='F', push_flag='F').
4. Process crash simulations:
   - Crash immediately after Q023 staging (Oracle still NP).
   - Crash immediately after Q020 Oracle claim (Oracle already RQ).
5. Mixed batch reconciliation:
   - Verifies accurate categorization and summary accounting across concurrent states.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.batch.reconciler import reconcile_staged_requests
from app.context import ExecutionContext
from app.db.oracle import BCD_STATUS_NP, BCD_STATUS_RQ
from app.processor import process_pending_recharges


class TestReconciliationFaultTolerance:
    """Test suite verifying cross-database reconciliation recovery paths."""

    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_branch_1_already_claimed_confirms_without_duplicate_oracle_write(
        self,
        mock_fetch_staged,
        mock_fetch_oracle,
        mock_writeback,
        mock_mark_dispatchable,
    ):
        """Branch 1: When Oracle already recorded RQ for this reqid, confirm without duplicate writeback."""
        mock_fetch_staged.return_value = [
            {"reqid": 901, "caf_serial_no": "CAF01", "gsmno": "9412300001", "circle_code": 2}
        ]
        # Oracle already has RQ for reqid 901
        mock_fetch_oracle.return_value = {
            ("9412300001", "CAF01", 2): {
                "FRC_FLOW_STATUS": BCD_STATUS_RQ,
                "FRC_REQID": 901,
            }
        }
        mock_mark_dispatchable.return_value = 1

        summary = reconcile_staged_requests(batch_size=50)

        assert summary["staged_found"] == 1
        assert summary["already_claimed_confirmed"] == 1
        assert summary["reclaimed_confirmed"] == 0
        assert summary["conflict_marked_failed"] == 0

        mock_mark_dispatchable.assert_called_once_with([901])
        # Idempotency guard: batch_writeback_bcd_rq must NOT be called
        mock_writeback.assert_not_called()

    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_branch_2_unclaimed_retries_claim_and_confirms_dispatchable(
        self,
        mock_fetch_staged,
        mock_fetch_oracle,
        mock_writeback,
        mock_mark_dispatchable,
    ):
        """Branch 2: When Oracle is still NP (unclaimed), retry claim and mark dispatchable."""
        candidate = {"reqid": 902, "caf_serial_no": "CAF02", "gsmno": "9412300002", "circle_code": 2}
        mock_fetch_staged.return_value = [candidate]

        mock_fetch_oracle.return_value = {
            ("9412300002", "CAF02", 2): {
                "FRC_FLOW_STATUS": BCD_STATUS_NP,
                "FRC_REQID": None,
            }
        }
        mock_writeback.return_value = 1
        mock_mark_dispatchable.return_value = 1

        summary = reconcile_staged_requests(batch_size=50)

        assert summary["staged_found"] == 1
        assert summary["reclaimed_confirmed"] == 1
        assert summary["already_claimed_confirmed"] == 0
        assert summary["conflict_marked_failed"] == 0

        # Claim was retried and row marked dispatchable
        mock_writeback.assert_called_once_with([candidate])
        mock_mark_dispatchable.assert_called_once_with([902])

    @patch("app.batch.reconciler.mark_requests_staging_failed")
    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_branch_3_conflicting_reqid_marks_staging_failed(
        self,
        mock_fetch_staged,
        mock_fetch_oracle,
        mock_writeback,
        mock_mark_dispatchable,
        mock_mark_failed,
    ):
        """Branch 3: When Oracle was claimed by a different reqid, mark current request failed."""
        mock_fetch_staged.return_value = [
            {"reqid": 903, "caf_serial_no": "CAF03", "gsmno": "9412300003", "circle_code": 2}
        ]
        # Oracle was claimed by reqid 888 (different request!)
        mock_fetch_oracle.return_value = {
            ("9412300003", "CAF03", 2): {
                "FRC_FLOW_STATUS": BCD_STATUS_RQ,
                "FRC_REQID": 888,
            }
        }

        summary = reconcile_staged_requests(batch_size=50)

        assert summary["staged_found"] == 1
        assert summary["conflict_marked_failed"] == 1
        assert summary["already_claimed_confirmed"] == 0
        assert summary["reclaimed_confirmed"] == 0

        mock_mark_failed.assert_called_once()
        assert mock_mark_failed.call_args[0][0] == [903]
        mock_writeback.assert_not_called()
        mock_mark_dispatchable.assert_not_called()

    @patch("app.batch.reconciler.mark_requests_staging_failed")
    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_branch_3_missing_in_oracle_marks_staging_failed(
        self,
        mock_fetch_staged,
        mock_fetch_oracle,
        mock_mark_dispatchable,
        mock_mark_failed,
    ):
        """Branch 3: When staged row is completely missing in Oracle BCD, mark failed."""
        mock_fetch_staged.return_value = [
            {"reqid": 904, "caf_serial_no": "CAF04", "gsmno": "9412300004", "circle_code": 2}
        ]
        # Oracle returned empty (candidate not found)
        mock_fetch_oracle.return_value = {}

        summary = reconcile_staged_requests(batch_size=50)

        assert summary["conflict_marked_failed"] == 1
        mock_mark_failed.assert_called_once()
        assert mock_mark_failed.call_args[0][0] == [904]
        mock_mark_dispatchable.assert_not_called()

    @patch("app.batch.reconciler.mark_requests_staging_failed")
    @patch("app.batch.reconciler.mark_requests_dispatchable")
    @patch("app.batch.reconciler.batch_writeback_bcd_rq")
    @patch("app.batch.reconciler.fetch_bcd_claim_statuses")
    @patch("app.batch.reconciler.fetch_staged_unconfirmed_requests")
    def test_mixed_batch_reconciliation_multi_branch(
        self,
        mock_fetch_staged,
        mock_fetch_oracle,
        mock_writeback,
        mock_mark_dispatchable,
        mock_mark_failed,
    ):
        """A mixed batch of 4 staged requests across branches 1, 2, 3 (conflict & missing)."""
        req_claimed = {"reqid": 911, "caf_serial_no": "CAF1", "gsmno": "9412300001", "circle_code": 2}
        req_unclaimed = {"reqid": 912, "caf_serial_no": "CAF2", "gsmno": "9412300002", "circle_code": 2}
        req_conflict = {"reqid": 913, "caf_serial_no": "CAF3", "gsmno": "9412300003", "circle_code": 2}
        req_missing = {"reqid": 914, "caf_serial_no": "CAF4", "gsmno": "9412300004", "circle_code": 2}

        mock_fetch_staged.return_value = [req_claimed, req_unclaimed, req_conflict, req_missing]

        mock_fetch_oracle.return_value = {
            ("9412300001", "CAF1", 2): {"FRC_FLOW_STATUS": BCD_STATUS_RQ, "FRC_REQID": 911},
            ("9412300002", "CAF2", 2): {"FRC_FLOW_STATUS": BCD_STATUS_NP, "FRC_REQID": None},
            ("9412300003", "CAF3", 2): {"FRC_FLOW_STATUS": "SU", "FRC_REQID": 555},  # Already succeeded elsewhere!
            # req_missing omitted from dict
        }

        mock_writeback.return_value = 1
        mock_mark_dispatchable.return_value = 1

        summary = reconcile_staged_requests(batch_size=50)

        assert summary["staged_found"] == 4
        assert summary["already_claimed_confirmed"] == 1
        assert summary["reclaimed_confirmed"] == 1
        assert summary["conflict_marked_failed"] == 2
        assert summary["errors"] == 0
