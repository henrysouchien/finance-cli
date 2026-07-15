ALTER TABLE liabilities ADD COLUMN intro_apr_end_date TEXT;

DROP TRIGGER IF EXISTS _sync_log_liabilities_insert;
DROP TRIGGER IF EXISTS _sync_log_liabilities_update;
DROP TRIGGER IF EXISTS _sync_log_liabilities_delete;

CREATE TRIGGER _sync_log_liabilities_insert
AFTER INSERT ON liabilities
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES (
        'liabilities',
        'INSERT',
        json(json_object('id', NEW.id)),
        NULL,
        json(json_object(
            'id', NEW.id,
            'account_id', NEW.account_id,
            'liability_type', NEW.liability_type,
            'is_active', NEW.is_active,
            'last_seen_at', NEW.last_seen_at,
            'is_overdue', NEW.is_overdue,
            'last_payment_amount_cents', NEW.last_payment_amount_cents,
            'last_payment_date', NEW.last_payment_date,
            'last_statement_balance_cents', NEW.last_statement_balance_cents,
            'last_statement_issue_date', NEW.last_statement_issue_date,
            'minimum_payment_cents', NEW.minimum_payment_cents,
            'next_payment_due_date', NEW.next_payment_due_date,
            'apr_purchase', NEW.apr_purchase,
            'apr_balance_transfer', NEW.apr_balance_transfer,
            'apr_cash_advance', NEW.apr_cash_advance,
            'intro_apr_end_date', NEW.intro_apr_end_date,
            'interest_rate_pct', NEW.interest_rate_pct,
            'origination_principal_cents', NEW.origination_principal_cents,
            'outstanding_interest_cents', NEW.outstanding_interest_cents,
            'expected_payoff_date', NEW.expected_payoff_date,
            'loan_name', NEW.loan_name,
            'loan_status_type', NEW.loan_status_type,
            'loan_status_end_date', NEW.loan_status_end_date,
            'repayment_plan_type', NEW.repayment_plan_type,
            'repayment_plan_description', NEW.repayment_plan_description,
            'servicer_name', NEW.servicer_name,
            'ytd_interest_paid_cents', NEW.ytd_interest_paid_cents,
            'ytd_principal_paid_cents', NEW.ytd_principal_paid_cents,
            'mortgage_rate_pct', NEW.mortgage_rate_pct,
            'mortgage_rate_type', NEW.mortgage_rate_type,
            'loan_term', NEW.loan_term,
            'maturity_date', NEW.maturity_date,
            'origination_date', NEW.origination_date,
            'escrow_balance_cents', NEW.escrow_balance_cents,
            'has_pmi', NEW.has_pmi,
            'has_prepayment_penalty', NEW.has_prepayment_penalty,
            'next_monthly_payment_cents', NEW.next_monthly_payment_cents,
            'past_due_amount_cents', NEW.past_due_amount_cents,
            'current_late_fee_cents', NEW.current_late_fee_cents,
            'property_address_json', NEW.property_address_json,
            'raw_plaid_json', NEW.raw_plaid_json,
            'fetched_at', NEW.fetched_at,
            'updated_at', NEW.updated_at
        )),
        current_session_id()
    );
END;

CREATE TRIGGER _sync_log_liabilities_update
AFTER UPDATE ON liabilities
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES (
        'liabilities',
        'UPDATE',
        json(json_object('id', NEW.id)),
        json(json_object(
            'id', OLD.id,
            'account_id', OLD.account_id,
            'liability_type', OLD.liability_type,
            'is_active', OLD.is_active,
            'last_seen_at', OLD.last_seen_at,
            'is_overdue', OLD.is_overdue,
            'last_payment_amount_cents', OLD.last_payment_amount_cents,
            'last_payment_date', OLD.last_payment_date,
            'last_statement_balance_cents', OLD.last_statement_balance_cents,
            'last_statement_issue_date', OLD.last_statement_issue_date,
            'minimum_payment_cents', OLD.minimum_payment_cents,
            'next_payment_due_date', OLD.next_payment_due_date,
            'apr_purchase', OLD.apr_purchase,
            'apr_balance_transfer', OLD.apr_balance_transfer,
            'apr_cash_advance', OLD.apr_cash_advance,
            'intro_apr_end_date', OLD.intro_apr_end_date,
            'interest_rate_pct', OLD.interest_rate_pct,
            'origination_principal_cents', OLD.origination_principal_cents,
            'outstanding_interest_cents', OLD.outstanding_interest_cents,
            'expected_payoff_date', OLD.expected_payoff_date,
            'loan_name', OLD.loan_name,
            'loan_status_type', OLD.loan_status_type,
            'loan_status_end_date', OLD.loan_status_end_date,
            'repayment_plan_type', OLD.repayment_plan_type,
            'repayment_plan_description', OLD.repayment_plan_description,
            'servicer_name', OLD.servicer_name,
            'ytd_interest_paid_cents', OLD.ytd_interest_paid_cents,
            'ytd_principal_paid_cents', OLD.ytd_principal_paid_cents,
            'mortgage_rate_pct', OLD.mortgage_rate_pct,
            'mortgage_rate_type', OLD.mortgage_rate_type,
            'loan_term', OLD.loan_term,
            'maturity_date', OLD.maturity_date,
            'origination_date', OLD.origination_date,
            'escrow_balance_cents', OLD.escrow_balance_cents,
            'has_pmi', OLD.has_pmi,
            'has_prepayment_penalty', OLD.has_prepayment_penalty,
            'next_monthly_payment_cents', OLD.next_monthly_payment_cents,
            'past_due_amount_cents', OLD.past_due_amount_cents,
            'current_late_fee_cents', OLD.current_late_fee_cents,
            'property_address_json', OLD.property_address_json,
            'raw_plaid_json', OLD.raw_plaid_json,
            'fetched_at', OLD.fetched_at,
            'updated_at', OLD.updated_at
        )),
        json(json_object(
            'id', NEW.id,
            'account_id', NEW.account_id,
            'liability_type', NEW.liability_type,
            'is_active', NEW.is_active,
            'last_seen_at', NEW.last_seen_at,
            'is_overdue', NEW.is_overdue,
            'last_payment_amount_cents', NEW.last_payment_amount_cents,
            'last_payment_date', NEW.last_payment_date,
            'last_statement_balance_cents', NEW.last_statement_balance_cents,
            'last_statement_issue_date', NEW.last_statement_issue_date,
            'minimum_payment_cents', NEW.minimum_payment_cents,
            'next_payment_due_date', NEW.next_payment_due_date,
            'apr_purchase', NEW.apr_purchase,
            'apr_balance_transfer', NEW.apr_balance_transfer,
            'apr_cash_advance', NEW.apr_cash_advance,
            'intro_apr_end_date', NEW.intro_apr_end_date,
            'interest_rate_pct', NEW.interest_rate_pct,
            'origination_principal_cents', NEW.origination_principal_cents,
            'outstanding_interest_cents', NEW.outstanding_interest_cents,
            'expected_payoff_date', NEW.expected_payoff_date,
            'loan_name', NEW.loan_name,
            'loan_status_type', NEW.loan_status_type,
            'loan_status_end_date', NEW.loan_status_end_date,
            'repayment_plan_type', NEW.repayment_plan_type,
            'repayment_plan_description', NEW.repayment_plan_description,
            'servicer_name', NEW.servicer_name,
            'ytd_interest_paid_cents', NEW.ytd_interest_paid_cents,
            'ytd_principal_paid_cents', NEW.ytd_principal_paid_cents,
            'mortgage_rate_pct', NEW.mortgage_rate_pct,
            'mortgage_rate_type', NEW.mortgage_rate_type,
            'loan_term', NEW.loan_term,
            'maturity_date', NEW.maturity_date,
            'origination_date', NEW.origination_date,
            'escrow_balance_cents', NEW.escrow_balance_cents,
            'has_pmi', NEW.has_pmi,
            'has_prepayment_penalty', NEW.has_prepayment_penalty,
            'next_monthly_payment_cents', NEW.next_monthly_payment_cents,
            'past_due_amount_cents', NEW.past_due_amount_cents,
            'current_late_fee_cents', NEW.current_late_fee_cents,
            'property_address_json', NEW.property_address_json,
            'raw_plaid_json', NEW.raw_plaid_json,
            'fetched_at', NEW.fetched_at,
            'updated_at', NEW.updated_at
        )),
        current_session_id()
    );
END;

CREATE TRIGGER _sync_log_liabilities_delete
AFTER DELETE ON liabilities
FOR EACH ROW
WHEN current_session_id() != '__STREAM__'
BEGIN
    INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)
    VALUES (
        'liabilities',
        'DELETE',
        json(json_object('id', OLD.id)),
        json(json_object(
            'id', OLD.id,
            'account_id', OLD.account_id,
            'liability_type', OLD.liability_type,
            'is_active', OLD.is_active,
            'last_seen_at', OLD.last_seen_at,
            'is_overdue', OLD.is_overdue,
            'last_payment_amount_cents', OLD.last_payment_amount_cents,
            'last_payment_date', OLD.last_payment_date,
            'last_statement_balance_cents', OLD.last_statement_balance_cents,
            'last_statement_issue_date', OLD.last_statement_issue_date,
            'minimum_payment_cents', OLD.minimum_payment_cents,
            'next_payment_due_date', OLD.next_payment_due_date,
            'apr_purchase', OLD.apr_purchase,
            'apr_balance_transfer', OLD.apr_balance_transfer,
            'apr_cash_advance', OLD.apr_cash_advance,
            'intro_apr_end_date', OLD.intro_apr_end_date,
            'interest_rate_pct', OLD.interest_rate_pct,
            'origination_principal_cents', OLD.origination_principal_cents,
            'outstanding_interest_cents', OLD.outstanding_interest_cents,
            'expected_payoff_date', OLD.expected_payoff_date,
            'loan_name', OLD.loan_name,
            'loan_status_type', OLD.loan_status_type,
            'loan_status_end_date', OLD.loan_status_end_date,
            'repayment_plan_type', OLD.repayment_plan_type,
            'repayment_plan_description', OLD.repayment_plan_description,
            'servicer_name', OLD.servicer_name,
            'ytd_interest_paid_cents', OLD.ytd_interest_paid_cents,
            'ytd_principal_paid_cents', OLD.ytd_principal_paid_cents,
            'mortgage_rate_pct', OLD.mortgage_rate_pct,
            'mortgage_rate_type', OLD.mortgage_rate_type,
            'loan_term', OLD.loan_term,
            'maturity_date', OLD.maturity_date,
            'origination_date', OLD.origination_date,
            'escrow_balance_cents', OLD.escrow_balance_cents,
            'has_pmi', OLD.has_pmi,
            'has_prepayment_penalty', OLD.has_prepayment_penalty,
            'next_monthly_payment_cents', OLD.next_monthly_payment_cents,
            'past_due_amount_cents', OLD.past_due_amount_cents,
            'current_late_fee_cents', OLD.current_late_fee_cents,
            'property_address_json', OLD.property_address_json,
            'raw_plaid_json', OLD.raw_plaid_json,
            'fetched_at', OLD.fetched_at,
            'updated_at', OLD.updated_at
        )),
        NULL,
        current_session_id()
    );
END;
