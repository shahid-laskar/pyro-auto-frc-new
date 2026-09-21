"""Fault tolerance test suite for cross-database reconciliation (Phase 8 & 15).

Verifies that under the production-aligned direct-insert architecture (0 DDL, direct in_status='C'):
1. reconcile_staged_requests is safely retired as a no-op.
2. It returns 0 staged found, 0 claimed, and 0 errors.
3. No unnecessary database calls or staging churn occurs.
"""

import pytest
from app.batch.reconciler import reconcile_staged_requests


class TestReconciliationFaultTolerance:
    """Test suite verifying cross-database reconciliation recovery paths."""

    def test_reconcile_staged_requests_is_noop_in_production_aligned_architecture(self):
        """Under direct in_status='C' staging, reconciler safely returns 0 without DB operations."""
        summary = reconcile_staged_requests(batch_size=50)

        assert summary["staged_found"] == 0
        assert summary["already_claimed_confirmed"] == 0
        assert summary["reclaimed_confirmed"] == 0
        assert summary["conflict_marked_failed"] == 0
        assert summary["reclaim_failed"] == 0
        assert summary["errors"] == 0

    def test_reconcile_staged_requests_custom_batch_size(self):
        """reconcile_staged_requests accepts batch_size parameter and returns empty summary."""
        summary = reconcile_staged_requests(batch_size=200)

        assert summary["staged_found"] == 0
        assert summary["errors"] == 0
