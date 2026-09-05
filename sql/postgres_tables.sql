CREATE TABLE IF NOT EXISTS public.frc_pyro_request_data (

    reqid BIGSERIAL NOT NULL,
    client_txn_id VARCHAR(15),

    caf_serial_no VARCHAR(30) NOT NULL,
    gsmno VARCHAR(10) NOT NULL,
    csccode VARCHAR(50),
    circle_code SMALLINT,
    kyc_mode VARCHAR(5) NOT NULL,
    edate DATE NOT NULL,

    frc_plan_name VARCHAR(100),
    frc_plan_code VARCHAR(50) NOT NULL,
    frc_category_code VARCHAR(50) NOT NULL,
    frcamt INTEGER NOT NULL,

    reqdate TIMESTAMPTZ,

    ctopup_number VARCHAR(10) NOT NULL,
    vendormsisdn VARCHAR(10),
    vendorid VARCHAR(50),

    mpin VARCHAR(200) NOT NULL,
    mpin_length SMALLINT,

    in_status VARCHAR(3) DEFAULT 'C' NOT NULL,
    pyro_status VARCHAR(3) DEFAULT 'N' NOT NULL,

    push_flag VARCHAR(1) DEFAULT 'N' NOT NULL,
    push_remarks VARCHAR(200),
    push_date TIMESTAMPTZ,

    batch_date DATE DEFAULT CURRENT_DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,

    pyro_trans_id BIGINT,
    pyro_initial_statuscode INTEGER,
    submitted_at TIMESTAMPTZ,
    msg2pyro TEXT,
    msg_afterreq TEXT,

    callback_received_at TIMESTAMPTZ,
    dealer_bal_before NUMERIC(12,2),
    dealer_bal_after NUMERIC(12,2),
    subscriber_circle VARCHAR(50),
    msg_aftertr TEXT,
    replymsg VARCHAR(200),
    replyrecvd_date TIMESTAMPTZ,

    status_check_eligible_at TIMESTAMPTZ,
    status_check_count SMALLINT DEFAULT 0 NOT NULL,
    last_status_check_at TIMESTAMPTZ,

    final_status VARCHAR(10),
    pyro_final_statuscode INTEGER,
    completed_at TIMESTAMPTZ,

    retry_count SMALLINT DEFAULT 0 NOT NULL,
    max_retries SMALLINT DEFAULT 3 NOT NULL,
    last_error_code VARCHAR(10),
    last_error_msg VARCHAR(500),

    seller_comm NUMERIC,
    fra_comm NUMERIC,

    ipdetail VARCHAR(20),
    remarks VARCHAR(500),
    updated_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,

    CONSTRAINT frc_pyro_pk PRIMARY KEY (reqid),

    CONSTRAINT frc_pyro_uq_batch_caf UNIQUE (batch_date, caf_serial_no),

    CONSTRAINT frc_pyro_kyc_chk CHECK (kyc_mode IN ('EKYC', 'DKYC', 'SKYC')),

    CONSTRAINT frc_pyro_in_status_chk CHECK (in_status IN ('C', 'S', 'F')),

    CONSTRAINT frc_pyro_pyro_status_chk CHECK (pyro_status IN ('N', 'REG', 'SUC', 'FAL')),

    CONSTRAINT frc_pyro_push_flag_chk CHECK (push_flag IN ('N', 'P', 'Y', 'F', 'E')),

    CONSTRAINT frc_pyro_final_chk CHECK (final_status IS NULL OR final_status IN ('SUCCESS', 'FAILED')),

    CONSTRAINT frc_pyro_retry_chk CHECK (retry_count <= max_retries + 1)
);

CREATE INDEX idx_frc_pyro_pickup
    ON public.frc_pyro_request_data (push_flag, batch_date, created_at);

CREATE INDEX idx_frc_pyro_trans_id
    ON public.frc_pyro_request_data (pyro_trans_id)
    WHERE pyro_trans_id IS NOT NULL;

CREATE INDEX idx_frc_pyro_status_check
    ON public.frc_pyro_request_data (status_check_eligible_at, push_flag)
    WHERE push_flag = 'P';

CREATE INDEX idx_frc_pyro_caf
    ON public.frc_pyro_request_data (caf_serial_no, batch_date);

CREATE INDEX idx_frc_pyro_gsmno
    ON public.frc_pyro_request_data (gsmno);

CREATE INDEX idx_frc_pyro_final
    ON public.frc_pyro_request_data (final_status, completed_at)
    WHERE final_status IS NOT NULL;



CREATE TABLE IF NOT EXISTS public.frc_txn_log (

    log_seq BIGSERIAL NOT NULL,

    frc_reqid BIGINT NOT NULL,
    caf_serial_no VARCHAR(30) NOT NULL,
    gsmno VARCHAR(10) NOT NULL,
    batch_date DATE NOT NULL,
    client_txn_id VARCHAR(15),

    api_stage VARCHAR(20) NOT NULL,
    api_endpoint VARCHAR(200),
    http_method VARCHAR(6),
    attempt_no SMALLINT DEFAULT 1 NOT NULL,

    request_headers VARCHAR(500),
    request_body TEXT,

    response_http_code SMALLINT,
    response_body TEXT,
    pyro_status_code INTEGER,
    pyro_status_text VARCHAR(50),
    pyro_txn_id BIGINT,

    call_started_at TIMESTAMPTZ NOT NULL,
    call_ended_at TIMESTAMPTZ,
    duration_ms INTEGER,

    is_success VARCHAR(1) NOT NULL,
    is_perm_failure VARCHAR(1) DEFAULT 'N' NOT NULL,

    error_class VARCHAR(200),
    error_detail TEXT,

    logged_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,

    CONSTRAINT frc_txn_log_pk PRIMARY KEY (log_seq),

    CONSTRAINT frc_txn_log_stage_chk CHECK (api_stage IN (
        'AUTH', 'REFRESH_TOKEN', 'ACTION_TOKEN',
        'RECHARGE', 'STATUS_CHECK', 'CALLBACK_RECV'
    )),

    CONSTRAINT frc_txn_log_success_chk CHECK (is_success IN ('Y', 'N')),

    CONSTRAINT frc_txn_log_perm_fail_chk CHECK (is_perm_failure IN ('Y', 'N'))
);

CREATE INDEX idx_txnlog_reqid
    ON public.frc_txn_log (frc_reqid, api_stage, logged_at);

CREATE INDEX idx_txnlog_errors
    ON public.frc_txn_log (batch_date, is_success, pyro_status_code);

CREATE INDEX idx_txnlog_pyro_txn
    ON public.frc_txn_log (pyro_txn_id)
    WHERE api_stage IN ('CALLBACK_RECV', 'STATUS_CHECK');

CREATE INDEX idx_txnlog_duration
    ON public.frc_txn_log (duration_ms, api_stage);

CREATE INDEX idx_txnlog_gsmno
    ON public.frc_txn_log (gsmno, logged_at);