import csv
import io
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

app = FastAPI(title="AFP Data Platform API", version="2.0.0")
APP_ROOT = Path(__file__).resolve().parent
STATIC_DIR = APP_ROOT / "static"
SCHEMA_PATH = APP_ROOT / "schema.sql"

CERT_OVERHEAD_RATE = Decimal("0.151")
CERT_PROFIT_RATE = Decimal("0.132")

CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*")
cors_allow_credentials = True
if CORS_ALLOW_ORIGINS.strip() == "*":
  cors_origins = ["*"]
  cors_allow_credentials = False
else:
  cors_origins = [origin.strip() for origin in CORS_ALLOW_ORIGINS.split(",") if origin.strip()]

app.add_middleware(
  CORSMiddleware,
  allow_origins=cors_origins,
  allow_credentials=cors_allow_credentials,
  allow_methods=["*"],
  allow_headers=["*"],
)


class AdminQueryRequest(BaseModel):
  sql: str = Field(..., min_length=1)
  max_rows: int = Field(default=500, ge=1, le=5000)


class CertificationUpdate(BaseModel):
  certified_without_fee: float = Field(..., ge=0)
  certification_status: str | None = None


def _required_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f"Missing required environment variable: {name}")
  return value


def _to_decimal(value: str | None, field_name: str) -> Decimal:
  try:
    raw = str(value).strip()
    if raw == "":
      return Decimal("0")
    if raw in {"-", "—", "–", "N/A", "NA", "n/a", "na"}:
      return Decimal("0")
    cleaned = raw.replace(",", "").replace("$", "").replace("CAD", "").strip()
    if cleaned in {"-", "—", "–", "N/A", "NA", "n/a", "na"}:
      return Decimal("0")
    if cleaned.startswith("(") and cleaned.endswith(")"):
      cleaned = f"-{cleaned[1:-1].strip()}"
    return Decimal(cleaned)
  except (InvalidOperation, ValueError, TypeError):
    raise HTTPException(status_code=400, detail=f"Invalid decimal value for {field_name}: {value}")


def _normalize_header(name: str) -> str:
  return " ".join(str(name).strip().lower().split())


def _normalize_row(row: dict[str, str]) -> dict[str, str]:
  return {_normalize_header(k): v for k, v in row.items()}


def _get(row: dict[str, str], *keys: str, default: str = "") -> str:
  for key in keys:
    k = _normalize_header(key)
    if k in row:
      return row[k]
  return default


def _parse_date(value: str | None, field_name: str):
  if value is None:
    return None
  raw = str(value).strip()
  if raw == "":
    return None

  cleaned = raw.replace(",", " ").strip()
  cleaned = cleaned.replace("  ", " ")
  for suffix in ("st", "nd", "rd", "th"):
    cleaned = cleaned.replace(suffix + " ", " ")

  formats = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%m/%d/%y",
    "%d-%b-%y",
    "%B %d %Y",
    "%b %d %Y",
  )
  for fmt in formats:
    try:
      return datetime.strptime(raw, fmt).date()
    except ValueError:
      pass
    try:
      return datetime.strptime(cleaned, fmt).date()
    except ValueError:
      continue

  try:
    return datetime.fromisoformat(raw).date()
  except ValueError:
    try:
      return datetime.fromisoformat(cleaned).date()
    except ValueError:
      raise HTTPException(status_code=400, detail=f"Invalid date value for {field_name}: {value}")


def _validate_readonly_sql(sql: str) -> str:
  normalized = " ".join(sql.strip().split())
  if not normalized:
    raise HTTPException(status_code=400, detail="SQL query cannot be empty")

  lowered = normalized.lower()
  if not (lowered.startswith("select ") or lowered.startswith("with ")):
    raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")

  forbidden = [
    "insert ", "update ", "delete ", "merge ", "drop ", "alter ", "create ",
    "truncate ", "execute ", "exec ", "grant ", "revoke ", "deny ",
  ]
  if any(token in lowered for token in forbidden):
    raise HTTPException(status_code=400, detail="Only read-only queries are allowed")

  if ";" in lowered:
    raise HTTPException(status_code=400, detail="Multiple statements are not allowed")

  return normalized


def get_sql_connection():
  import pymssql

  return pymssql.connect(
    server=_required_env("SQL_HOST"),
    user=_required_env("SQL_USER"),
    password=_required_env("SQL_PASSWORD"),
    database=_required_env("SQL_DATABASE"),
    as_dict=True,
  )


def _load_schema_sql() -> str:
  if not SCHEMA_PATH.exists():
    raise RuntimeError(f"Schema file not found: {SCHEMA_PATH}")
  return SCHEMA_PATH.read_text(encoding="utf-8")


def ensure_schema_exists() -> None:
  ddl = _load_schema_sql()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(ddl)
    conn.commit()


@app.get("/health")
def health():
  return {"status": "ok", "timestamp_utc": datetime.now(timezone.utc).isoformat()}


@app.get("/", include_in_schema=False)
def ui_home():
  index_file = STATIC_DIR / "index.html"
  if not index_file.exists():
    raise HTTPException(status_code=500, detail="Frontend UI not found")
  return FileResponse(index_file)


def _read_csv_upload(file: UploadFile) -> list[dict[str, str]]:
  if not file.filename.lower().endswith(".csv"):
    raise HTTPException(status_code=400, detail="Only .csv files are supported")

  content = file.file.read()
  if not content:
    raise HTTPException(status_code=400, detail="Uploaded file is empty")

  try:
    decoded = content.decode("utf-8-sig")
  except UnicodeDecodeError as exc:
    raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc

  reader = csv.DictReader(io.StringIO(decoded))
  if not reader.fieldnames:
    raise HTTPException(status_code=400, detail="CSV has no headers")

  rows = []
  for row in reader:
    rows.append(_normalize_row(row))
  if not rows:
    raise HTTPException(status_code=400, detail="CSV has no data rows")
  return rows


@app.post("/po-master")
def upload_po_master(file: UploadFile = File(...)):
  ensure_schema_exists()
  rows = _read_csv_upload(file)

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for row in rows:
        po_no = _get(row, "PO No")
        if not po_no:
          continue
        vendor = _get(row, "Vendor Name")
        currency = _get(row, "Currency")
        po_value_original = _get(row, "PO Value in Original Currency")
        po_value_cad = _get(row, "Converted_PO_Value_in_CAD", "PO Value in Original CAD")
        po_value_original_dec = _to_decimal(po_value_original or "0", "PO Value in Original Currency")
        po_value_cad_dec = _to_decimal(po_value_cad or "0", "Converted_PO_Value_in_CAD")

        cursor.execute(
          """
          MERGE dbo.po_master AS target
          USING (SELECT %s AS po_no) AS src
          ON target.po_no = src.po_no
          WHEN MATCHED THEN
            UPDATE SET
              vendor_name = %s,
              currency = %s,
              po_value_original = %s,
              po_value_cad = %s,
              remaining = CASE
                WHEN po_value_cad IS NULL THEN remaining
                ELSE po_value_cad - total_claimed
              END,
              updated_at = SYSUTCDATETIME()
          WHEN NOT MATCHED THEN
            INSERT (po_no, vendor_name, currency, po_value_original, po_value_cad, total_claimed, remaining, updated_at)
            VALUES (%s, %s, %s, %s, %s, 0, %s, SYSUTCDATETIME());
          """,
          (
            po_no,
            vendor or None,
            currency or None,
            float(po_value_original_dec),
            float(po_value_cad_dec),
            po_no,
            vendor or None,
            currency or None,
            float(po_value_original_dec),
            float(po_value_cad_dec),
            float(po_value_cad_dec),
          ),
        )
    conn.commit()

  return {"message": "PO master updated", "rows": len(rows)}


@app.post("/cycles/{itb_no}/upload/itb-cost-performance")
def upload_itb_cost_performance(itb_no: str, file: UploadFile = File(...)):
  ensure_schema_exists()
  rows = _read_csv_upload(file)

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for row in rows:
        ln_itm_id = _get(row, "Ln_ITM_ID")
        if not ln_itm_id:
          continue

        cursor.execute(
          """
          INSERT INTO dbo.input_itb_cost_performance (
            itb_no, ln_itm_id, bundle_id, cbs_1, cbs_2, cbs_3, cbs_4, cbs_5, cost_type,
            submitted_actual_cost, submitted_1_fc, submitted_2_fc, submitted_3_fc, variance_current_submission
          )
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """,
          (
            itb_no,
            ln_itm_id,
            _get(row, "Bundle_ID") or None,
            _get(row, "CBS_1") or None,
            _get(row, "CBS_2") or None,
            _get(row, "CBS_3") or None,
            _get(row, "CBS_4") or None,
            _get(row, "CBS_5") or None,
            _get(row, "Cost_Type") or None,
            float(_to_decimal(_get(row, "Submitted_ Actual_Cost"), "Submitted_ Actual_Cost")) if _get(row, "Submitted_ Actual_Cost") else None,
            float(_to_decimal(_get(row, "Submitted_1_FC"), "Submitted_1_FC")) if _get(row, "Submitted_1_FC") else None,
            float(_to_decimal(_get(row, "Submitted_2_FC"), "Submitted_2_FC")) if _get(row, "Submitted_2_FC") else None,
            float(_to_decimal(_get(row, "Submitted_3_FC"), "Submitted_3_FC")) if _get(row, "Submitted_3_FC") else None,
            float(_to_decimal(_get(row, "Variance_Current_Submission"), "Variance_Current_Submission")) if _get(row, "Variance_Current_Submission") else None,
          ),
        )

        budget_at_completion = _get(row, "Budget_at_Completion")
        overhead = _get(row, "Overhead")
        profit = _get(row, "Profit")
        budget_plus_fee = _get(row, "Budget_plus_Fee")
        submitted_ltd_wo = _get(row, "Submitted_ActualCosts_LTD_without_fees")
        submitted_ltd_oh = _get(row, "Submitted_ActualCosts_LTD_Overhead")
        submitted_ltd_fee = _get(row, "Submitted_ActualCosts_LTD_Fee")
        submitted_ltd_w = _get(row, "Submitted_ActualCosts_LTD_with_fees")
        certified_ltd_wo = _get(row, "Certified_ActualCosts_LTD_without_fees")
        certified_ltd_oh = _get(row, "Certified_ActualCosts_LTD_Overhead")
        certified_ltd_fee = _get(row, "Certified_ActualCosts_LTD_Fee")
        certified_ltd_w = _get(row, "Certified_ActualCosts_LTD_with_fees")
        variance_ltd = _get(row, "Variance_LTD")
        total_variance = _get(row, "Total Variance")
        variance_at_completion = _get(row, "Variance_at_Completion")
        estimate_at_completion = _get(row, "Estimate_at_Completion")
        estimate_to_complete = _get(row, "Estimate_to_Complete")
        ltd_certified = _get(row, "LTD_Certified_ with_Current_AFP")

        update_params = (
          _get(row, "Bundle_ID") or None,
          _get(row, "CBS_1") or None,
          _get(row, "CBS_2") or None,
          _get(row, "CBS_3") or None,
          _get(row, "CBS_4") or None,
          _get(row, "CBS_5") or None,
          _get(row, "Cost_Type") or None,
          float(_to_decimal(budget_at_completion, "Budget_at_Completion")) if budget_at_completion else None,
          float(_to_decimal(overhead, "Overhead")) if overhead else None,
          float(_to_decimal(profit, "Profit")) if profit else None,
          float(_to_decimal(budget_plus_fee, "Budget_plus_Fee")) if budget_plus_fee else None,
          float(_to_decimal(submitted_ltd_wo, "Submitted_ActualCosts_LTD_without_fees")) if submitted_ltd_wo else None,
          float(_to_decimal(submitted_ltd_oh, "Submitted_ActualCosts_LTD_Overhead")) if submitted_ltd_oh else None,
          float(_to_decimal(submitted_ltd_fee, "Submitted_ActualCosts_LTD_Fee")) if submitted_ltd_fee else None,
          float(_to_decimal(submitted_ltd_w, "Submitted_ActualCosts_LTD_with_fees")) if submitted_ltd_w else None,
          float(_to_decimal(certified_ltd_wo, "Certified_ActualCosts_LTD_without_fees")) if certified_ltd_wo else None,
          float(_to_decimal(certified_ltd_oh, "Certified_ActualCosts_LTD_Overhead")) if certified_ltd_oh else None,
          float(_to_decimal(certified_ltd_fee, "Certified_ActualCosts_LTD_Fee")) if certified_ltd_fee else None,
          float(_to_decimal(certified_ltd_w, "Certified_ActualCosts_LTD_with_fees")) if certified_ltd_w else None,
          float(_to_decimal(variance_ltd, "Variance_LTD")) if variance_ltd else None,
          float(_to_decimal(total_variance, "Total Variance")) if total_variance else None,
          float(_to_decimal(variance_at_completion, "Variance_at_Completion")) if variance_at_completion else None,
          float(_to_decimal(estimate_at_completion, "Estimate_at_Completion")) if estimate_at_completion else None,
          float(_to_decimal(estimate_to_complete, "Estimate_to_Complete")) if estimate_to_complete else None,
          float(_to_decimal(ltd_certified, "LTD_Certified_ with_Current_AFP")) if ltd_certified else None,
          itb_no,
          ln_itm_id,
        )

        cursor.execute(
          """
          UPDATE dbo.itb_line_master
          SET
            bundle_id = %s,
            cbs_1 = %s,
            cbs_2 = %s,
            cbs_3 = %s,
            cbs_4 = %s,
            cbs_5 = %s,
            cost_type = %s,
            budget_at_completion = %s,
            overhead = %s,
            profit = %s,
            budget_plus_fee = %s,
            submitted_actualcosts_ltd_without_fees = %s,
            submitted_actualcosts_ltd_overhead = %s,
            submitted_actualcosts_ltd_fee = %s,
            submitted_actualcosts_ltd_with_fees = %s,
            certified_actualcosts_ltd_without_fees = %s,
            certified_actualcosts_ltd_overhead = %s,
            certified_actualcosts_ltd_fee = %s,
            certified_actualcosts_ltd_with_fees = %s,
            variance_ltd = %s,
            total_variance = %s,
            variance_at_completion = %s,
            estimate_at_completion = %s,
            estimate_to_complete = %s,
            ltd_certified_with_current_afp = %s,
            last_itb_no = %s,
            updated_at = SYSUTCDATETIME()
          WHERE ln_itm_id = %s
          """,
          update_params,
        )

        if cursor.rowcount == 0:
          cursor.execute(
            """
            INSERT INTO dbo.itb_line_master (
              ln_itm_id, bundle_id, cbs_1, cbs_2, cbs_3, cbs_4, cbs_5, cost_type,
              budget_at_completion, overhead, profit, budget_plus_fee,
              submitted_actualcosts_ltd_without_fees, submitted_actualcosts_ltd_overhead, submitted_actualcosts_ltd_fee, submitted_actualcosts_ltd_with_fees,
              certified_actualcosts_ltd_without_fees, certified_actualcosts_ltd_overhead, certified_actualcosts_ltd_fee, certified_actualcosts_ltd_with_fees,
              variance_ltd, total_variance, variance_at_completion, estimate_at_completion, estimate_to_complete,
              ltd_certified_with_current_afp, last_itb_no, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, SYSUTCDATETIME())
            """,
            (
              ln_itm_id,
              _get(row, "Bundle_ID") or None,
              _get(row, "CBS_1") or None,
              _get(row, "CBS_2") or None,
              _get(row, "CBS_3") or None,
              _get(row, "CBS_4") or None,
              _get(row, "CBS_5") or None,
              _get(row, "Cost_Type") or None,
              float(_to_decimal(budget_at_completion, "Budget_at_Completion")) if budget_at_completion else None,
              float(_to_decimal(overhead, "Overhead")) if overhead else None,
              float(_to_decimal(profit, "Profit")) if profit else None,
              float(_to_decimal(budget_plus_fee, "Budget_plus_Fee")) if budget_plus_fee else None,
              float(_to_decimal(submitted_ltd_wo, "Submitted_ActualCosts_LTD_without_fees")) if submitted_ltd_wo else None,
              float(_to_decimal(submitted_ltd_oh, "Submitted_ActualCosts_LTD_Overhead")) if submitted_ltd_oh else None,
              float(_to_decimal(submitted_ltd_fee, "Submitted_ActualCosts_LTD_Fee")) if submitted_ltd_fee else None,
              float(_to_decimal(submitted_ltd_w, "Submitted_ActualCosts_LTD_with_fees")) if submitted_ltd_w else None,
              float(_to_decimal(certified_ltd_wo, "Certified_ActualCosts_LTD_without_fees")) if certified_ltd_wo else None,
              float(_to_decimal(certified_ltd_oh, "Certified_ActualCosts_LTD_Overhead")) if certified_ltd_oh else None,
              float(_to_decimal(certified_ltd_fee, "Certified_ActualCosts_LTD_Fee")) if certified_ltd_fee else None,
              float(_to_decimal(certified_ltd_w, "Certified_ActualCosts_LTD_with_fees")) if certified_ltd_w else None,
              float(_to_decimal(variance_ltd, "Variance_LTD")) if variance_ltd else None,
              float(_to_decimal(total_variance, "Total Variance")) if total_variance else None,
              float(_to_decimal(variance_at_completion, "Variance_at_Completion")) if variance_at_completion else None,
              float(_to_decimal(estimate_at_completion, "Estimate_at_Completion")) if estimate_at_completion else None,
              float(_to_decimal(estimate_to_complete, "Estimate_to_Complete")) if estimate_to_complete else None,
              float(_to_decimal(ltd_certified, "LTD_Certified_ with_Current_AFP")) if ltd_certified else None,
              itb_no,
            ),
          )

    conn.commit()

  return {"message": "ITB cost performance ingested", "rows": len(rows)}


@app.post("/cycles/{itb_no}/upload/erp-actuals")
def upload_erp_actuals(itb_no: str, file: UploadFile = File(...)):
  ensure_schema_exists()
  rows = _read_csv_upload(file)

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for row in rows:
        cost_id = _get(row, "Cost_ID")
        if not cost_id:
          continue
        cursor.execute(
          """
          INSERT INTO dbo.input_erp_actuals (
            itb_no, ln_itm_id, cost_id, bundle_id, cbs_1, cbs_2, cbs_3, cbs_4, cbs_5,
            vendor_name, reimbursement_type, cost_type, activity, activity_name,
            cost_id_description, cost_element_category_ref, submitted_acwp, submitted_oh, submitted_profit, submitted_acwp_w_fee
          )
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """,
          (
            itb_no,
            _get(row, "Ln_ITM_ID") or None,
            cost_id,
            _get(row, "Bundle_ID") or None,
            _get(row, "CBS_1") or None,
            _get(row, "CBS_2") or None,
            _get(row, "CBS_3") or None,
            _get(row, "CBS_4") or None,
            _get(row, "CBS_5") or None,
            _get(row, "Vendor_Name") or None,
            _get(row, "Reimbursement_Type") or None,
            _get(row, "Cost_Type") or None,
            _get(row, "Activity") or None,
            _get(row, "Activity_Name") or None,
            _get(row, "Cost_ID_Description") or None,
            _get(row, "Cost_Element_Category_Ref") or None,
            float(_to_decimal(_get(row, "Submitted_ACWP"), "Submitted_ACWP")) if _get(row, "Submitted_ACWP") else None,
            float(_to_decimal(_get(row, "Submitted_OH"), "Submitted_OH")) if _get(row, "Submitted_OH") else None,
            float(_to_decimal(_get(row, "Submitted_Profit"), "Submitted_Profit")) if _get(row, "Submitted_Profit") else None,
            float(_to_decimal(_get(row, "Submitted_ACWP_w_Fee"), "Submitted_ACWP_w_Fee")) if _get(row, "Submitted_ACWP_w_Fee") else None,
          ),
        )
    conn.commit()

  return {"message": "ERP actuals ingested", "rows": len(rows)}


@app.post("/cycles/{itb_no}/upload/invoice-information")
def upload_invoice_information(itb_no: str, file: UploadFile = File(...)):
  ensure_schema_exists()
  rows = _read_csv_upload(file)

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for row in rows:
        cost_id = _get(row, "Cost_ID")
        if not cost_id:
          continue
        cursor.execute(
          """
          INSERT INTO dbo.input_invoice_information (
            itb_no, cost_id, vendor_name, actual_or_accrual, invoice_no, invoice_date, po_no,
            currency, subtotal_amount, fx, amount_cad, claim_amount
          )
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """,
          (
            itb_no,
            cost_id,
            _get(row, "Vendor Name") or None,
            _get(row, "Actual/Accruals") or None,
            _get(row, "Invoice No") or None,
            _parse_date(_get(row, "Invoice Date"), "Invoice Date"),
            _get(row, "PO No") or None,
            _get(row, "Currency") or None,
            float(_to_decimal(_get(row, "Subtotal Amount (Without Tax)"), "Subtotal Amount")) if _get(row, "Subtotal Amount (Without Tax)") else None,
            float(_to_decimal(_get(row, "FX"), "FX")) if _get(row, "FX") else None,
            float(_to_decimal(_get(row, "Amount in CAD"), "Amount in CAD")) if _get(row, "Amount in CAD") else None,
            float(_to_decimal(_get(row, "Claim Amount"), "Claim Amount")) if _get(row, "Claim Amount") else None,
          ),
        )
    conn.commit()

  return {"message": "Invoice information ingested", "rows": len(rows)}


@app.post("/cycles/{itb_no}/process")
def process_cycle(itb_no: str):
  ensure_schema_exists()

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(
        """
        MERGE dbo.submission_cycle AS target
        USING (SELECT %s AS itb_no) AS src
        ON target.itb_no = src.itb_no
        WHEN NOT MATCHED THEN
          INSERT (itb_no) VALUES (%s);
        """,
        (itb_no, itb_no),
      )

      cursor.execute(
        """
        SELECT id, cost_id, po_no, amount_cad, claim_amount, vendor_name, actual_or_accrual,
               invoice_no, invoice_date, currency, subtotal_amount, fx
        FROM dbo.input_invoice_information
        WHERE itb_no = %s
        ORDER BY id
        """,
        (itb_no,),
      )
      invoice_rows = cursor.fetchall()

      for row in invoice_rows:
        claim_amount = Decimal(str(row.get("claim_amount") or 0))
        amount_cad = Decimal(str(row.get("amount_cad") or 0))
        authorization_status = "authorized"

        if claim_amount <= 0:
          authorization_status = "invalid_amount"
          authorized_amount = Decimal("0")
          unauthorized_amount = claim_amount
        elif claim_amount > amount_cad:
          authorization_status = "invalid_amount"
          authorized_amount = Decimal("0")
          unauthorized_amount = claim_amount
        else:
          cursor.execute(
            """
            SELECT po_no, total_claimed, remaining
            FROM dbo.po_master WITH (UPDLOCK, ROWLOCK)
            WHERE po_no = %s
            """,
            (row.get("po_no"),),
          )
          po_row = cursor.fetchone()
          if not po_row:
            authorization_status = "deauthorized"
            authorized_amount = Decimal("0")
            unauthorized_amount = claim_amount
          else:
            remaining = Decimal(str(po_row.get("remaining") or 0))
            if remaining <= 0:
              authorization_status = "deauthorized"
              authorized_amount = Decimal("0")
              unauthorized_amount = claim_amount
            else:
              authorized_amount = min(claim_amount, remaining)
              unauthorized_amount = claim_amount - authorized_amount
              if unauthorized_amount > 0:
                authorization_status = "partially_authorized"

              cursor.execute(
                """
                UPDATE dbo.po_master
                SET total_claimed = total_claimed + %s,
                    remaining = remaining - %s,
                    last_itb_no = %s,
                    updated_at = SYSUTCDATETIME()
                WHERE po_no = %s
                """,
                (float(authorized_amount), float(authorized_amount), itb_no, row.get("po_no")),
              )

              cursor.execute(
                """
                INSERT INTO dbo.txn_po_ledger (itb_no, po_no, claimed_amount, source)
                VALUES (%s, %s, %s, %s)
                """,
                (itb_no, row.get("po_no"), float(authorized_amount), "invoice"),
              )

        cursor.execute(
          """
          INSERT INTO dbo.txn_invoice_information (
            itb_no, cost_id, vendor_name, actual_or_accrual, invoice_no, invoice_date, po_no,
            currency, subtotal_amount, fx, amount_cad, claim_amount,
            authorized_amount, unauthorized_amount, authorization_status
          )
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """,
          (
            itb_no,
            row.get("cost_id"),
            row.get("vendor_name"),
            row.get("actual_or_accrual"),
            row.get("invoice_no"),
            row.get("invoice_date"),
            row.get("po_no"),
            row.get("currency"),
            row.get("subtotal_amount"),
            row.get("fx"),
            row.get("amount_cad"),
            row.get("claim_amount"),
            float(authorized_amount),
            float(unauthorized_amount),
            authorization_status,
          ),
        )

      cursor.execute(
        """
        SELECT cost_id, SUM(authorized_amount) AS authorized_total
        FROM dbo.txn_invoice_information
        WHERE itb_no = %s
        GROUP BY cost_id
        """,
        (itb_no,),
      )
      authorized_by_cost = {row["cost_id"]: Decimal(str(row["authorized_total"] or 0)) for row in cursor.fetchall()}
      remaining_by_cost = dict(authorized_by_cost)

      cursor.execute(
        """
        SELECT * FROM dbo.input_erp_actuals
        WHERE itb_no = %s
        ORDER BY id
        """,
        (itb_no,),
      )
      erp_rows = cursor.fetchall()

      for row in erp_rows:
        cost_id = row.get("cost_id")
        authorized_total = remaining_by_cost.get(cost_id, Decimal("0"))
        submitted_acwp = Decimal(str(row.get("submitted_acwp") or 0))
        if authorized_total <= 0 or submitted_acwp <= 0:
          authorized_cost = Decimal("0")
          status = "deauthorized"
        else:
          authorized_cost = min(submitted_acwp, authorized_total)
          remaining_by_cost[cost_id] = authorized_total - authorized_cost
          status = "authorized" if authorized_cost == submitted_acwp else "partially_authorized"

        certified_without_fee = authorized_cost
        certified_overhead = (certified_without_fee * CERT_OVERHEAD_RATE).quantize(Decimal("0.01"))
        certified_profit = (certified_without_fee * CERT_PROFIT_RATE).quantize(Decimal("0.01"))
        certified_amount_w_fee = certified_without_fee + certified_overhead + certified_profit

        cursor.execute(
          """
          INSERT INTO dbo.txn_erp_actuals (
            itb_no, ln_itm_id, cost_id, bundle_id, cbs_1, cbs_2, cbs_3, cbs_4, cbs_5,
            vendor_name, reimbursement_type, cost_type, activity, activity_name,
            cost_id_description, cost_element_category_ref,
            submitted_acwp, submitted_oh, submitted_profit, submitted_acwp_w_fee,
            authorized_cost_amount, certification_status,
            certified_without_fee, certified_overhead, certified_profit, certified_amount_w_fee
          )
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """,
          (
            itb_no,
            row.get("ln_itm_id"),
            cost_id,
            row.get("bundle_id"),
            row.get("cbs_1"),
            row.get("cbs_2"),
            row.get("cbs_3"),
            row.get("cbs_4"),
            row.get("cbs_5"),
            row.get("vendor_name"),
            row.get("reimbursement_type"),
            row.get("cost_type"),
            row.get("activity"),
            row.get("activity_name"),
            row.get("cost_id_description"),
            row.get("cost_element_category_ref"),
            row.get("submitted_acwp"),
            row.get("submitted_oh"),
            row.get("submitted_profit"),
            row.get("submitted_acwp_w_fee"),
            float(authorized_cost),
            status,
            float(certified_without_fee),
            float(certified_overhead),
            float(certified_profit),
            float(certified_amount_w_fee),
          ),
        )

      cursor.execute(
        """
        SELECT ln_itm_id,
               SUM(COALESCE(submitted_acwp, 0)) AS submitted_actual_cost,
               SUM(COALESCE(authorized_cost_amount, 0)) AS certified_actual_cost
        FROM dbo.txn_erp_actuals
        WHERE itb_no = %s
        GROUP BY ln_itm_id
        """,
        (itb_no,),
      )
      erp_totals = {row["ln_itm_id"]: row for row in cursor.fetchall()}

      cursor.execute(
        """
        SELECT * FROM dbo.input_itb_cost_performance
        WHERE itb_no = %s
        ORDER BY id
        """,
        (itb_no,),
      )
      itb_rows = cursor.fetchall()

      for row in itb_rows:
        ln_itm_id = row.get("ln_itm_id")
        totals = erp_totals.get(ln_itm_id, {"submitted_actual_cost": 0, "certified_actual_cost": 0})
        submitted_actual_cost_calc = Decimal(str(totals.get("submitted_actual_cost") or 0))
        certified_actual_cost = Decimal(str(totals.get("certified_actual_cost") or 0))

        cursor.execute(
          """
          SELECT ltd_certified_with_current_afp
          FROM dbo.itb_line_master
          WHERE ln_itm_id = %s
          """,
          (ln_itm_id,),
        )
        prior = cursor.fetchone()
        prior_ltd = Decimal(str(prior.get("ltd_certified_with_current_afp") or 0)) if prior else Decimal("0")
        ltd = prior_ltd + certified_actual_cost

        forecast_total = None
        if row.get("submitted_1_fc") is not None or row.get("submitted_2_fc") is not None or row.get("submitted_3_fc") is not None:
          forecast_total = (
            Decimal(str(row.get("submitted_1_fc") or 0)) +
            Decimal(str(row.get("submitted_2_fc") or 0)) +
            Decimal(str(row.get("submitted_3_fc") or 0))
          )

        cursor.execute(
          """
          INSERT INTO dbo.txn_itb_cost_performance (
            itb_no, ln_itm_id, bundle_id, cbs_1, cbs_2, cbs_3, cbs_4, cbs_5, cost_type,
            submitted_actual_cost, submitted_1_fc, submitted_2_fc, submitted_3_fc, variance_current_submission,
            forecast_total, submitted_actual_cost_calc, certified_actual_cost, ltd_certified_with_current_afp
          )
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """,
          (
            itb_no,
            ln_itm_id,
            row.get("bundle_id"),
            row.get("cbs_1"),
            row.get("cbs_2"),
            row.get("cbs_3"),
            row.get("cbs_4"),
            row.get("cbs_5"),
            row.get("cost_type"),
            row.get("submitted_actual_cost"),
            row.get("submitted_1_fc"),
            row.get("submitted_2_fc"),
            row.get("submitted_3_fc"),
            row.get("variance_current_submission"),
            float(forecast_total) if forecast_total is not None else None,
            float(submitted_actual_cost_calc),
            float(certified_actual_cost),
            float(ltd),
          ),
        )

        cursor.execute(
          """
          UPDATE dbo.itb_line_master
          SET ltd_certified_with_current_afp = %s,
              last_itb_no = %s,
              updated_at = SYSUTCDATETIME()
          WHERE ln_itm_id = %s
          """,
          (float(ltd), itb_no, ln_itm_id),
        )

    conn.commit()

  return {"message": "Cycle processed", "itb_no": itb_no}


@app.get("/txn/invoices")
def get_txn_invoices(itb_no: str = Query(...)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.txn_invoice_information WHERE itb_no = %s", (itb_no,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/txn/erp")
def get_txn_erp(itb_no: str = Query(...)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.txn_erp_actuals WHERE itb_no = %s", (itb_no,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/txn/itb")
def get_txn_itb(itb_no: str = Query(...)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.txn_itb_cost_performance WHERE itb_no = %s", (itb_no,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/po-master")
def get_po_master():
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.po_master ORDER BY po_no")
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/itb-line-master")
def get_itb_line_master(limit: int = Query(default=200, ge=1, le=5000)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT TOP (%s) * FROM dbo.itb_line_master ORDER BY ln_itm_id", (limit,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/input/invoices")
def get_input_invoices(itb_no: str = Query(...)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.input_invoice_information WHERE itb_no = %s", (itb_no,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/input/erp")
def get_input_erp(itb_no: str = Query(...)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.input_erp_actuals WHERE itb_no = %s", (itb_no,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/input/itb")
def get_input_itb(itb_no: str = Query(...)):
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT * FROM dbo.input_itb_cost_performance WHERE itb_no = %s", (itb_no,))
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.post("/admin/query")
def admin_query(request: AdminQueryRequest):
  ensure_schema_exists()
  sql = _validate_readonly_sql(request.sql)
  max_rows = request.max_rows

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SET ROWCOUNT %s", (max_rows,))
      cursor.execute(sql)
      rows = cursor.fetchall()
      cursor.execute("SET ROWCOUNT 0")

  return {"count": len(rows), "records": rows}


@app.patch("/txn/erp/{row_id}/certification")
def update_erp_certification(row_id: int, payload: CertificationUpdate):
  ensure_schema_exists()
  certified_without_fee = Decimal(str(payload.certified_without_fee))
  if certified_without_fee < 0:
    raise HTTPException(status_code=400, detail="Certified amount must be >= 0")

  certified_overhead = (certified_without_fee * CERT_OVERHEAD_RATE).quantize(Decimal("0.01"))
  certified_profit = (certified_without_fee * CERT_PROFIT_RATE).quantize(Decimal("0.01"))
  certified_amount_w_fee = certified_without_fee + certified_overhead + certified_profit

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(
        "SELECT itb_no, ln_itm_id FROM dbo.txn_erp_actuals WHERE id = %s",
        (row_id,),
      )
      row = cursor.fetchone()
      if not row:
        raise HTTPException(status_code=404, detail="ERP transaction not found")

      itb_no = row.get("itb_no")
      ln_itm_id = row.get("ln_itm_id")
      status = payload.certification_status or "certified"

      cursor.execute(
        """
        UPDATE dbo.txn_erp_actuals
        SET certified_without_fee = %s,
            certified_overhead = %s,
            certified_profit = %s,
            certified_amount_w_fee = %s,
            certification_status = %s,
            processed_at = SYSUTCDATETIME()
        WHERE id = %s
        """,
        (
          float(certified_without_fee),
          float(certified_overhead),
          float(certified_profit),
          float(certified_amount_w_fee),
          status,
          row_id,
        ),
      )

      cursor.execute(
        """
        SELECT
          SUM(COALESCE(submitted_acwp, 0)) AS submitted_actual_cost,
          SUM(COALESCE(certified_without_fee, 0)) AS certified_actual_cost
        FROM dbo.txn_erp_actuals
        WHERE itb_no = %s AND ln_itm_id = %s
        """,
        (itb_no, ln_itm_id),
      )
      totals = cursor.fetchone() or {}
      submitted_actual_cost = Decimal(str(totals.get("submitted_actual_cost") or 0))
      certified_actual_cost = Decimal(str(totals.get("certified_actual_cost") or 0))

      cursor.execute(
        """
        UPDATE dbo.txn_itb_cost_performance
        SET submitted_actual_cost_calc = %s,
            certified_actual_cost = %s
        WHERE itb_no = %s AND ln_itm_id = %s
        """,
        (float(submitted_actual_cost), float(certified_actual_cost), itb_no, ln_itm_id),
      )

      cursor.execute(
        """
        SELECT SUM(COALESCE(certified_actual_cost, 0)) AS ltd
        FROM dbo.txn_itb_cost_performance
        WHERE ln_itm_id = %s
        """,
        (ln_itm_id,),
      )
      ltd_row = cursor.fetchone() or {}
      ltd_total = Decimal(str(ltd_row.get("ltd") or 0))

      cursor.execute(
        """
        UPDATE dbo.txn_itb_cost_performance
        SET ltd_certified_with_current_afp = %s
        WHERE itb_no = %s AND ln_itm_id = %s
        """,
        (float(ltd_total), itb_no, ln_itm_id),
      )

      cursor.execute(
        """
        UPDATE dbo.itb_line_master
        SET ltd_certified_with_current_afp = %s,
            updated_at = SYSUTCDATETIME()
        WHERE ln_itm_id = %s
        """,
        (float(ltd_total), ln_itm_id),
      )

    conn.commit()

  return {
    "message": "Certification updated",
    "id": row_id,
    "itb_no": itb_no,
    "ln_itm_id": ln_itm_id,
    "certified_without_fee": float(certified_without_fee),
    "certified_overhead": float(certified_overhead),
    "certified_profit": float(certified_profit),
    "certified_amount_w_fee": float(certified_amount_w_fee),
  }
