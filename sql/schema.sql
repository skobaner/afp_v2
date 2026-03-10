IF OBJECT_ID(N'dbo.submission_cycle', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.submission_cycle (
    cycle_id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    cycle_code NVARCHAR(100) NULL,
    submitted_at DATETIME2(3) NOT NULL CONSTRAINT DF_submission_cycle_submitted_at DEFAULT SYSUTCDATETIME(),
    source_system NVARCHAR(100) NULL,
    notes NVARCHAR(1000) NULL,
    CONSTRAINT UQ_submission_cycle_itb_no UNIQUE (itb_no)
  );
END;

IF OBJECT_ID(N'dbo.po_master', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.po_master (
    po_no NVARCHAR(100) NOT NULL PRIMARY KEY,
    vendor_name NVARCHAR(255) NULL,
    currency NVARCHAR(16) NULL,
    po_value_original DECIMAL(18,2) NULL,
    po_value_cad DECIMAL(18,2) NULL,
    total_claimed DECIMAL(18,2) NOT NULL CONSTRAINT DF_po_master_total_claimed DEFAULT (0),
    remaining DECIMAL(18,2) NOT NULL CONSTRAINT DF_po_master_remaining DEFAULT (0),
    last_itb_no NVARCHAR(100) NULL,
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_po_master_updated_at DEFAULT SYSUTCDATETIME()
  );
END;

IF OBJECT_ID(N'dbo.itb_line_master', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.itb_line_master (
    ln_itm_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    bundle_id NVARCHAR(255) NULL,
    cbs_1 NVARCHAR(255) NULL,
    cbs_2 NVARCHAR(255) NULL,
    cbs_3 NVARCHAR(255) NULL,
    cbs_4 NVARCHAR(255) NULL,
    cbs_5 NVARCHAR(255) NULL,
    cost_type NVARCHAR(100) NULL,
    budget_at_completion DECIMAL(18,2) NULL,
    overhead DECIMAL(18,2) NULL,
    profit DECIMAL(18,2) NULL,
    budget_plus_fee DECIMAL(18,2) NULL,
    submitted_actualcosts_ltd_without_fees DECIMAL(18,2) NULL,
    submitted_actualcosts_ltd_overhead DECIMAL(18,2) NULL,
    submitted_actualcosts_ltd_fee DECIMAL(18,2) NULL,
    submitted_actualcosts_ltd_with_fees DECIMAL(18,2) NULL,
    certified_actualcosts_ltd_without_fees DECIMAL(18,2) NULL,
    certified_actualcosts_ltd_overhead DECIMAL(18,2) NULL,
    certified_actualcosts_ltd_fee DECIMAL(18,2) NULL,
    certified_actualcosts_ltd_with_fees DECIMAL(18,2) NULL,
    variance_ltd DECIMAL(18,2) NULL,
    total_variance DECIMAL(18,2) NULL,
    variance_at_completion DECIMAL(18,2) NULL,
    estimate_at_completion DECIMAL(18,2) NULL,
    estimate_to_complete DECIMAL(18,2) NULL,
    ltd_certified_with_current_afp DECIMAL(18,2) NULL,
    last_itb_no NVARCHAR(100) NULL,
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_itb_line_master_updated_at DEFAULT SYSUTCDATETIME()
  );
END;

IF OBJECT_ID(N'dbo.input_itb_cost_performance', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.input_itb_cost_performance (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    ln_itm_id NVARCHAR(100) NOT NULL,
    bundle_id NVARCHAR(255) NULL,
    cbs_1 NVARCHAR(255) NULL,
    cbs_2 NVARCHAR(255) NULL,
    cbs_3 NVARCHAR(255) NULL,
    cbs_4 NVARCHAR(255) NULL,
    cbs_5 NVARCHAR(255) NULL,
    cost_type NVARCHAR(100) NULL,
    submitted_actual_cost DECIMAL(18,2) NULL,
    submitted_1_fc DECIMAL(18,2) NULL,
    submitted_2_fc DECIMAL(18,2) NULL,
    submitted_3_fc DECIMAL(18,2) NULL,
    variance_current_submission DECIMAL(18,2) NULL,
    ingested_at DATETIME2(3) NOT NULL CONSTRAINT DF_input_itb_cost_perf_ingested_at DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_input_itb_cost_perf_itb_ln ON dbo.input_itb_cost_performance (itb_no, ln_itm_id);
END;

IF OBJECT_ID(N'dbo.input_erp_actuals', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.input_erp_actuals (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    ln_itm_id NVARCHAR(100) NULL,
    cost_id NVARCHAR(100) NOT NULL,
    bundle_id NVARCHAR(255) NULL,
    cbs_1 NVARCHAR(255) NULL,
    cbs_2 NVARCHAR(255) NULL,
    cbs_3 NVARCHAR(255) NULL,
    cbs_4 NVARCHAR(255) NULL,
    cbs_5 NVARCHAR(255) NULL,
    vendor_name NVARCHAR(255) NULL,
    reimbursement_type NVARCHAR(255) NULL,
    cost_type NVARCHAR(100) NULL,
    activity NVARCHAR(255) NULL,
    activity_name NVARCHAR(255) NULL,
    cost_id_description NVARCHAR(500) NULL,
    cost_element_category_ref NVARCHAR(255) NULL,
    submitted_acwp DECIMAL(18,2) NULL,
    submitted_oh DECIMAL(18,2) NULL,
    submitted_profit DECIMAL(18,2) NULL,
    submitted_acwp_w_fee DECIMAL(18,2) NULL,
    ingested_at DATETIME2(3) NOT NULL CONSTRAINT DF_input_erp_actuals_ingested_at DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_input_erp_actuals_itb_cost ON dbo.input_erp_actuals (itb_no, cost_id);
  CREATE INDEX IX_input_erp_actuals_itb_ln ON dbo.input_erp_actuals (itb_no, ln_itm_id);
END;

IF OBJECT_ID(N'dbo.input_invoice_information', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.input_invoice_information (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    cost_id NVARCHAR(100) NOT NULL,
    vendor_name NVARCHAR(255) NULL,
    actual_or_accrual NVARCHAR(50) NULL,
    invoice_no NVARCHAR(100) NULL,
    invoice_date DATE NULL,
    po_no NVARCHAR(100) NULL,
    currency NVARCHAR(16) NULL,
    subtotal_amount DECIMAL(18,2) NULL,
    fx DECIMAL(18,6) NULL,
    amount_cad DECIMAL(18,2) NULL,
    claim_amount DECIMAL(18,2) NULL,
    ingested_at DATETIME2(3) NOT NULL CONSTRAINT DF_input_invoice_info_ingested_at DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_input_invoice_info_itb_cost ON dbo.input_invoice_information (itb_no, cost_id);
  CREATE INDEX IX_input_invoice_info_itb_po ON dbo.input_invoice_information (itb_no, po_no);
END;

IF OBJECT_ID(N'dbo.txn_invoice_information', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.txn_invoice_information (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    cost_id NVARCHAR(100) NOT NULL,
    vendor_name NVARCHAR(255) NULL,
    actual_or_accrual NVARCHAR(50) NULL,
    invoice_no NVARCHAR(100) NULL,
    invoice_date DATE NULL,
    po_no NVARCHAR(100) NULL,
    currency NVARCHAR(16) NULL,
    subtotal_amount DECIMAL(18,2) NULL,
    fx DECIMAL(18,6) NULL,
    amount_cad DECIMAL(18,2) NULL,
    claim_amount DECIMAL(18,2) NULL,
    authorized_amount DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_invoice_auth_amount DEFAULT (0),
    unauthorized_amount DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_invoice_unauth_amount DEFAULT (0),
    authorization_status NVARCHAR(32) NOT NULL,
    processed_at DATETIME2(3) NOT NULL CONSTRAINT DF_txn_invoice_processed_at DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_txn_invoice_info_itb_cost ON dbo.txn_invoice_information (itb_no, cost_id);
  CREATE INDEX IX_txn_invoice_info_itb_po ON dbo.txn_invoice_information (itb_no, po_no);
END;

IF OBJECT_ID(N'dbo.txn_erp_actuals', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.txn_erp_actuals (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    ln_itm_id NVARCHAR(100) NULL,
    cost_id NVARCHAR(100) NOT NULL,
    bundle_id NVARCHAR(255) NULL,
    cbs_1 NVARCHAR(255) NULL,
    cbs_2 NVARCHAR(255) NULL,
    cbs_3 NVARCHAR(255) NULL,
    cbs_4 NVARCHAR(255) NULL,
    cbs_5 NVARCHAR(255) NULL,
    vendor_name NVARCHAR(255) NULL,
    reimbursement_type NVARCHAR(255) NULL,
    cost_type NVARCHAR(100) NULL,
    activity NVARCHAR(255) NULL,
    activity_name NVARCHAR(255) NULL,
    cost_id_description NVARCHAR(500) NULL,
    cost_element_category_ref NVARCHAR(255) NULL,
    submitted_acwp DECIMAL(18,2) NULL,
    submitted_oh DECIMAL(18,2) NULL,
    submitted_profit DECIMAL(18,2) NULL,
    submitted_acwp_w_fee DECIMAL(18,2) NULL,
    authorized_cost_amount DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_erp_auth_cost DEFAULT (0),
    certification_status NVARCHAR(32) NOT NULL,
    certified_without_fee DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_erp_cert_wo_fee DEFAULT (0),
    certified_overhead DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_erp_cert_oh DEFAULT (0),
    certified_profit DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_erp_cert_profit DEFAULT (0),
    certified_amount_w_fee DECIMAL(18,2) NOT NULL CONSTRAINT DF_txn_erp_cert_w_fee DEFAULT (0),
    processed_at DATETIME2(3) NOT NULL CONSTRAINT DF_txn_erp_processed_at DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_txn_erp_actuals_itb_cost ON dbo.txn_erp_actuals (itb_no, cost_id);
  CREATE INDEX IX_txn_erp_actuals_itb_ln ON dbo.txn_erp_actuals (itb_no, ln_itm_id);
END;

IF OBJECT_ID(N'dbo.txn_itb_cost_performance', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.txn_itb_cost_performance (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    ln_itm_id NVARCHAR(100) NOT NULL,
    bundle_id NVARCHAR(255) NULL,
    cbs_1 NVARCHAR(255) NULL,
    cbs_2 NVARCHAR(255) NULL,
    cbs_3 NVARCHAR(255) NULL,
    cbs_4 NVARCHAR(255) NULL,
    cbs_5 NVARCHAR(255) NULL,
    cost_type NVARCHAR(100) NULL,
    submitted_actual_cost DECIMAL(18,2) NULL,
    submitted_1_fc DECIMAL(18,2) NULL,
    submitted_2_fc DECIMAL(18,2) NULL,
    submitted_3_fc DECIMAL(18,2) NULL,
    variance_current_submission DECIMAL(18,2) NULL,
    forecast_total DECIMAL(18,2) NULL,
    submitted_actual_cost_calc DECIMAL(18,2) NULL,
    certified_actual_cost DECIMAL(18,2) NULL,
    ltd_certified_with_current_afp DECIMAL(18,2) NULL,
    processed_at DATETIME2(3) NOT NULL CONSTRAINT DF_txn_itb_cost_processed_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_txn_itb_cost_itb_ln UNIQUE (itb_no, ln_itm_id)
  );
END;

IF OBJECT_ID(N'dbo.txn_po_ledger', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.txn_po_ledger (
    id INT IDENTITY(1,1) PRIMARY KEY,
    itb_no NVARCHAR(100) NOT NULL,
    po_no NVARCHAR(100) NOT NULL,
    claimed_amount DECIMAL(18,2) NOT NULL,
    source NVARCHAR(50) NULL,
    created_at DATETIME2(3) NOT NULL CONSTRAINT DF_txn_po_ledger_created_at DEFAULT SYSUTCDATETIME()
  );
  CREATE INDEX IX_txn_po_ledger_itb_po ON dbo.txn_po_ledger (itb_no, po_no);
END;
