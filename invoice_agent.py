from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, TypedDict
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver  # in-memory checkpointer


# -------------------------------------------------------------------
# 1. WORKFLOW STATE
# -------------------------------------------------------------------

class WorkflowState(TypedDict, total=False):
    # Input
    invoice_payload: Dict[str, Any]

    # INTAKE
    raw_id: str
    ingest_ts: str
    validated: bool

    # UNDERSTAND
    parsed_invoice: Dict[str, Any]

    # PREPARE
    vendor_profile: Dict[str, Any]
    normalized_invoice: Dict[str, Any]
    flags: Dict[str, Any]

    # RETRIEVE
    matched_pos: List[Dict[str, Any]]
    matched_grns: List[Dict[str, Any]]
    history: List[Dict[str, Any]]

    # MATCH_TWO_WAY
    match_score: float
    match_result: Literal["MATCHED", "FAILED"]
    tolerance_pct: float
    match_evidence: Dict[str, Any]

    # CHECKPOINT_HITL
    hitl_checkpoint_id: Optional[str]   # avoid reserved channel name "checkpoint_id"
    review_url: Optional[str]
    paused_reason: Optional[str]

    # HITL_DECISION
    human_decision: Optional[Literal["ACCEPT", "REJECT"]]
    reviewer_id: Optional[str]
    resume_token: Optional[str]
    next_stage: Optional[str]

    # RECONCILE
    accounting_entries: List[Dict[str, Any]]
    reconciliation_report: Dict[str, Any]

    # APPROVE
    approval_status: str
    approver_id: Optional[str]

    # POSTING
    posted: bool
    erp_txn_id: Optional[str]
    scheduled_payment_id: Optional[str]

    # NOTIFY
    notify_status: Dict[str, Any]
    notified_parties: List[str]

    # COMPLETE
    final_payload: Dict[str, Any]
    audit_log: List[Dict[str, Any]]
    status: str  # RUNNING / PAUSED / COMPLETED / MANUAL_HANDOFF / ERROR

    # Generic logging
    logs: List[str]


# -------------------------------------------------------------------
# 2. BIGTOOL + MCP CLIENT ABSTRACTIONS
# -------------------------------------------------------------------

class BigtoolPicker:
    def __init__(self, pools: Dict[str, List[str]]):
        self.pools = pools

    def select(self, capability: str, context: Dict[str, Any]) -> str:
        pool = self.pools.get(capability, [])
        if not pool:
            raise ValueError(f"No pool configured for capability={capability}")

        # Simple heuristic: high amount → 'sap_sandbox' ERP
        if capability == "erp_connector" and context.get("amount", 0) > 10000 and "sap_sandbox" in pool:
            choice = "sap_sandbox"
        else:
            choice = pool[0]

        return choice


class MCPClient:
    """
    Simple stub for MCP client routing.
    In real solution this wraps MCP protocol to talk to COMMON / ATLAS.
    """

    def __init__(self, name: str):
        self.name = name

    def call_ability(self, ability: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        # MOCK IMPLEMENTATIONS; replace with real MCP calls.

        # INTAKE
        if ability == "persist_raw_invoice":
            return {
                "raw_id": str(uuid.uuid4()),
                "ingest_ts": datetime.utcnow().isoformat()
            }

        # UNDERSTAND
        if ability == "parse_line_items":
            inv = payload["invoice_payload"]
            return{
                "invoice_text": " ".join([li["desc"] for li in inv.get("line_items", [])]),
                "parsed_line_items": inv.get("line_items", []),
                "detected_pos": [],
                "currency": inv.get("currency"),
                "parsed_dates": {
                    "invoice_date": inv.get("invoice_date"),
                    "due_date": inv.get("due_date"),
                },
            }

        # PREPARE
        if ability == "normalize_vendor_and_flags":
            vendor = payload["vendor_name"]
            return {
                "vendor_profile": {
                    "normalized_name": vendor.upper().strip(),
                    "tax_id": payload.get("vendor_tax_id"),
                    "enrichment_meta": {"source": "mock_enrichment"}
                },
                "normalized_invoice": {
                    "amount": payload["amount"],
                    "currency": payload["currency"],
                    "line_items": payload["line_items"]
                },
                "flags": {
                    "missing_info": [],
                    "risk_score": 0.1
                }
            }

        if ability == "enrich_vendor":
            return {
                "enrichment_meta": {
                    "credit_score": 780,
                    "risk_band": "LOW",
                    "provider": payload.get("provider")
                }
            }

        # RETRIEVE – **this is where we force some invoices to mismatch**
        if ability == "fetch_erp_data":
            invoice_amount = payload["amount"]

            # 💡 Logic:
            # - If amount <= 9000 → PO matches invoice (good match)
            # - If amount > 9000 → PO is 20% lower (bad match → triggers HITL)
            if invoice_amount <= 9000:
                po_amount = invoice_amount
            else:
                po_amount = round(invoice_amount * 0.8, 2)

            return {
                "matched_pos": [{"po_id": "PO123", "amount": po_amount}],
                "matched_grns": [{"grn_id": "GRN456"}],
                "history": [{"invoice_id": "INV-OLD-1", "amount": invoice_amount}]
            }

        # MATCH_TWO_WAY
        if ability == "compute_match_score":
            invoice_amount = payload["invoice_amount"]
            po_amount = payload["po_amount"]
            diff_pct = abs(invoice_amount - po_amount) / max(po_amount, 1) * 100
            score = max(0.0, 1.0 - diff_pct / 100.0)
            return {"match_score": score, "tolerance_pct": diff_pct}

        if ability == "post_process_match":
            return {"match_evidence": {"comment": "Mock evidence"}}

        # RECONCILE
        if ability == "build_accounting_entries":
            amount = payload["amount"]
            return {
                "accounting_entries": [
                    {"account": "AP", "debit": 0, "credit": amount},
                    {"account": "EXPENSE", "debit": amount, "credit": 0}
                ]
            }

        if ability == "build_reconciliation_report":
            return {"reconciliation_report": {"status": "OK"}}

        # APPROVE
        if ability == "apply_invoice_approval_policy":
            amount = payload["amount"]
            if amount <= 5000:
                return {"approval_status": "AUTO_APPROVED", "approver_id": None}
            return {"approval_status": "ESCALATED", "approver_id": "manager_1"}

        # POSTING
        if ability == "schedule_payment":
            return {
                "posted": True,
                "erp_txn_id": "ERP-TXN-123",
                "scheduled_payment_id": "PAY-789"
            }

        # NOTIFY
        if ability == "notify_finance_team":
            return {
                "notify_status": {"finance_team": "SENT"},
                "notified_parties": ["finance_team"]
            }

        if ability == "select_email_provider":
            return {"provider": payload.get("provider", "sendgrid")}

        # Default stub
        return {}


# -------------------------------------------------------------------
# 3. DB HELPERS FOR CHECKPOINT / HUMAN REVIEW QUEUE
# -------------------------------------------------------------------

HITL_DB_PATH = "./demo.db"


def init_db():
    conn = sqlite3.connect(HITL_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checkpoints (
            checkpoint_id TEXT PRIMARY KEY,
            invoice_id TEXT,
            vendor_name TEXT,
            amount REAL,
            created_at TEXT,
            reason_for_hold TEXT,
            review_url TEXT,
            state_blob TEXT,
            resolved INTEGER DEFAULT 0
        )
        """
    )
    conn.commit()
    conn.close()


def save_checkpoint(checkpoint_id: str, state: WorkflowState, reason: str, review_url: str):
    payload = state["invoice_payload"]
    conn = sqlite3.connect(HITL_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO checkpoints (
            checkpoint_id, invoice_id, vendor_name, amount,
            created_at, reason_for_hold, review_url, state_blob, resolved
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (
            checkpoint_id,
            payload.get("invoice_id"),
            payload.get("vendor_name"),
            float(payload.get("amount", 0)),
            datetime.utcnow().isoformat(),
            reason,
            review_url,
            json.dumps(state),
        ),
    )
    conn.commit()
    conn.close()


def list_pending_checkpoints() -> List[Dict[str, Any]]:
    conn = sqlite3.connect(HITL_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT checkpoint_id, invoice_id, vendor_name, amount,
               created_at, reason_for_hold, review_url
        FROM checkpoints
        WHERE resolved = 0
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    items = []
    for r in rows:
        items.append(
            {
                "checkpoint_id": r[0],
                "invoice_id": r[1],
                "vendor_name": r[2],
                "amount": r[3],
                "created_at": r[4],
                "reason_for_hold": r[5],
                "review_url": r[6],
            }
        )
    return items


def get_checkpoint_state(checkpoint_id: str) -> WorkflowState:
    conn = sqlite3.connect(HITL_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT state_blob FROM checkpoints WHERE checkpoint_id = ? AND resolved = 0",
        (checkpoint_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise KeyError("Checkpoint not found or already resolved")
    return json.loads(row[0])


def mark_checkpoint_resolved(checkpoint_id: str):
    conn = sqlite3.connect(HITL_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE checkpoints SET resolved = 1 WHERE checkpoint_id = ?",
        (checkpoint_id,),
    )
    conn.commit()
    conn.close()


# -------------------------------------------------------------------
# 4. NODE IMPLEMENTATIONS
# -------------------------------------------------------------------

MATCH_THRESHOLD = 0.90
APP_BASE_URL = "http://localhost:8000"

bigtool = BigtoolPicker(
    pools={
        "storage": ["local_fs", "s3", "gcs"],
        "ocr": ["google_vision", "tesseract", "aws_textract"],
        "enrichment": ["clearbit", "people_data_labs", "vendor_db"],
        "erp_connector": ["sap_sandbox", "netsuite", "mock_erp"],
        "db": ["sqlite", "postgres", "dynamodb"],
        "email": ["sendgrid", "smartlead", "ses"],
    }
)
common_client = MCPClient("COMMON")
atlas_client = MCPClient("ATLAS")


def log(state: WorkflowState, message: str) -> None:
    if "logs" not in state:
        state["logs"] = []
    ts = datetime.utcnow().isoformat()
    state["logs"].append(f"[{ts}] {message}")


# 1. INTAKE
def intake_node(state: WorkflowState) -> WorkflowState:
    invoice = state["invoice_payload"]
    required = ["invoice_id", "vendor_name", "amount", "currency"]
    missing = [f for f in required if f not in invoice]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    storage_backend = bigtool.select("storage", {})
    log(state, f"INTAKE: Bigtool selected storage={storage_backend}")

    result = common_client.call_ability("persist_raw_invoice", {"invoice_payload": invoice})
    state["raw_id"] = result["raw_id"]
    state["ingest_ts"] = result["ingest_ts"]
    state["validated"] = True
    log(state, f"INTAKE: Persisted raw invoice raw_id={state['raw_id']}")
    state["status"] = "RUNNING"
    return state


# 2. UNDERSTAND
def understand_node(state: WorkflowState) -> WorkflowState:
    invoice = state["invoice_payload"]
    ocr_tool = bigtool.select("ocr", {"attachments": invoice.get("attachments", [])})
    log(state, f"UNDERSTAND: Bigtool selected OCR={ocr_tool} via ATLAS")

    parsed = common_client.call_ability("parse_line_items", {"invoice_payload": invoice})
    state["parsed_invoice"] = parsed
    log(state, "UNDERSTAND: Parsed invoice line items and dates")
    return state


# 3. PREPARE
def prepare_node(state: WorkflowState) -> WorkflowState:
    invoice = state["invoice_payload"]
    enrichment_provider = bigtool.select("enrichment", {"vendor_name": invoice["vendor_name"]})
    log(state, f"PREPARE: Bigtool selected enrichment provider={enrichment_provider}")

    base = common_client.call_ability(
        "normalize_vendor_and_flags",
        {
            "vendor_name": invoice["vendor_name"],
            "vendor_tax_id": invoice.get("vendor_tax_id"),
            "amount": invoice["amount"],
            "currency": invoice["currency"],
            "line_items": invoice.get("line_items", []),
        },
    )
    enrich = atlas_client.call_ability(
        "enrich_vendor",
        {
            "vendor_name": invoice["vendor_name"],
            "tax_id": invoice.get("vendor_tax_id"),
            "provider": enrichment_provider,
        },
    )

    vendor_profile = base["vendor_profile"]
    vendor_profile["enrichment_meta"].update(enrich["enrichment_meta"])

    state["vendor_profile"] = vendor_profile
    state["normalized_invoice"] = base["normalized_invoice"]
    state["flags"] = base["flags"]
    log(state, "PREPARE: Normalized vendor and enriched vendor profile")
    return state


# 4. RETRIEVE
def retrieve_node(state: WorkflowState) -> WorkflowState:
    invoice = state["normalized_invoice"]
    erp_choice = bigtool.select("erp_connector", {"amount": invoice["amount"]})
    log(state, f"RETRIEVE: Bigtool selected ERP connector={erp_choice}")

    erp_data = atlas_client.call_ability(
        "fetch_erp_data",
        {
            "amount": invoice["amount"],
            "vendor_name": state["vendor_profile"]["normalized_name"],
            "erp": erp_choice,
        },
    )
    state["matched_pos"] = erp_data["matched_pos"]
    state["matched_grns"] = erp_data["matched_grns"]
    state["history"] = erp_data["history"]
    log(state, "RETRIEVE: Fetched POs, GRNs, and invoice history from ERP")
    return state


# 5. MATCH_TWO_WAY
def match_two_way_node(state: WorkflowState) -> WorkflowState:
    invoice_amount = state["normalized_invoice"]["amount"]
    first_po_amount = state["matched_pos"][0]["amount"] if state["matched_pos"] else invoice_amount

    match_result = common_client.call_ability(
        "compute_match_score",
        {"invoice_amount": invoice_amount, "po_amount": first_po_amount},
    )
    post = common_client.call_ability("post_process_match", {})

    state["match_score"] = match_result["match_score"]
    state["tolerance_pct"] = match_result["tolerance_pct"]
    state["match_evidence"] = post["match_evidence"]

    if state["match_score"] >= MATCH_THRESHOLD:
        state["match_result"] = "MATCHED"
    else:
        state["match_result"] = "FAILED"

    log(
        state,
        f"MATCH_TWO_WAY: match_score={state['match_score']:.3f}, "
        f"tolerance_pct={state['tolerance_pct']:.2f}, result={state['match_result']}",
    )
    return state


def route_after_match(state: WorkflowState) -> str:
    return "FAILED" if state["match_result"] == "FAILED" else "MATCHED"


# 6. CHECKPOINT_HITL
def checkpoint_hitl_node(state: WorkflowState) -> WorkflowState:
    checkpoint_id = str(uuid.uuid4())
    review_url = f"{APP_BASE_URL}/human-review/{checkpoint_id}"
    reason = "Two-way match score below threshold"

    save_checkpoint(checkpoint_id, state, reason, review_url)

    state["hitl_checkpoint_id"] = checkpoint_id
    state["review_url"] = review_url
    state["paused_reason"] = reason
    state["status"] = "PAUSED"
    log(state, f"CHECKPOINT_HITL: Created checkpoint_id={checkpoint_id}, review_url={review_url}")
    return state


# 7. HITL_DECISION
def hitl_decision_node(state: WorkflowState) -> WorkflowState:
    decision = state.get("human_decision")
    if decision not in ("ACCEPT", "REJECT"):
        raise ValueError("HITL_DECISION: human_decision must be ACCEPT or REJECT")

    if decision == "ACCEPT":
        state["next_stage"] = "RECONCILE"
        state["resume_token"] = str(uuid.uuid4())
        log(state, f"HITL_DECISION: ACCEPT by reviewer={state.get('reviewer_id')}")
    else:
        state["next_stage"] = "COMPLETE"
        state["status"] = "MANUAL_HANDOFF"
        state["resume_token"] = str(uuid.uuid4())
        log(state, f"HITL_DECISION: REJECT by reviewer={state.get('reviewer_id')}")

    return state


def route_after_hitl(state: WorkflowState) -> str:
    return state["human_decision"] or "REJECT"


# 8. RECONCILE
def reconcile_node(state: WorkflowState) -> WorkflowState:
    amount = state["normalized_invoice"]["amount"]
    acct = common_client.call_ability("build_accounting_entries", {"amount": amount})
    report = common_client.call_ability("build_reconciliation_report", {})
    state["accounting_entries"] = acct["accounting_entries"]
    state["reconciliation_report"] = report["reconciliation_report"]
    log(state, "RECONCILE: Built accounting entries and reconciliation report")
    return state


# 9. APPROVE
def approve_node(state: WorkflowState) -> WorkflowState:
    amount = state["normalized_invoice"]["amount"]
    result = atlas_client.call_ability("apply_invoice_approval_policy", {"amount": amount})
    state["approval_status"] = result["approval_status"]
    state["approver_id"] = result["approver_id"]
    log(
        state,
        f"APPROVE: approval_status={state['approval_status']}, approver_id={state['approver_id']}",
    )
    return state


# 10. POSTING
def posting_node(state: WorkflowState) -> WorkflowState:
    amount = state["normalized_invoice"]["amount"]
    erp_choice = bigtool.select("erp_connector", {"amount": amount})
    log(state, f"POSTING: Bigtool selected ERP={erp_choice}")

    result = atlas_client.call_ability(
        "schedule_payment",
        {"amount": amount, "erp": erp_choice},
    )
    state["posted"] = result["posted"]
    state["erp_txn_id"] = result["erp_txn_id"]
    state["scheduled_payment_id"] = result["scheduled_payment_id"]
    log(
        state,
        f"POSTING: posted={state['posted']}, erp_txn_id={state['erp_txn_id']}, "
        f"scheduled_payment_id={state['scheduled_payment_id']}",
    )
    return state


# 11. NOTIFY
def notify_node(state: WorkflowState) -> WorkflowState:
    provider = bigtool.select("email", {})
    log(state, f"NOTIFY: Bigtool selected email provider={provider}")
    finance = atlas_client.call_ability("notify_finance_team", {})
    state["notify_status"] = finance["notify_status"]
    state["notified_parties"] = finance["notified_parties"]
    log(state, "NOTIFY: Sent notifications to finance team (vendor email mocked)")
    return state


# 12. COMPLETE
def complete_node(state: WorkflowState) -> WorkflowState:
    state["final_payload"] = {
        "invoice": state["invoice_payload"],
        "vendor_profile": state.get("vendor_profile"),
        "match_score": state.get("match_score"),
        "match_result": state.get("match_result"),
        "accounting_entries": state.get("accounting_entries", []),
        "approval_status": state.get("approval_status"),
        "posting": {
            "posted": state.get("posted"),
            "erp_txn_id": state.get("erp_txn_id"),
            "scheduled_payment_id": state.get("scheduled_payment_id"),
        },
        "notifications": {
            "notify_status": state.get("notify_status", {}),
            "notified_parties": state.get("notified_parties", []),
        },
    }
    state["audit_log"] = state.get("logs", [])
    if state.get("status") != "MANUAL_HANDOFF":
        state["status"] = "COMPLETED"
    log(state, f"COMPLETE: Workflow finished with status={state['status']}")
    return state


# -------------------------------------------------------------------
# 5. GRAPH BUILDER
# -------------------------------------------------------------------

def build_graph(entry_node: str = "INTAKE"):
    graph = StateGraph(WorkflowState)

    graph.add_node("INTAKE", intake_node)
    graph.add_node("UNDERSTAND", understand_node)
    graph.add_node("PREPARE", prepare_node)
    graph.add_node("RETRIEVE", retrieve_node)
    graph.add_node("MATCH_TWO_WAY", match_two_way_node)
    graph.add_node("CHECKPOINT_HITL", checkpoint_hitl_node)
    graph.add_node("HITL_DECISION", hitl_decision_node)
    graph.add_node("RECONCILE", reconcile_node)
    graph.add_node("APPROVE", approve_node)
    graph.add_node("POSTING", posting_node)
    graph.add_node("NOTIFY", notify_node)
    graph.add_node("COMPLETE", complete_node)

    graph.set_entry_point(entry_node)

    graph.add_edge("INTAKE", "UNDERSTAND")
    graph.add_edge("UNDERSTAND", "PREPARE")
    graph.add_edge("PREPARE", "RETRIEVE")
    graph.add_edge("RETRIEVE", "MATCH_TWO_WAY")

    graph.add_conditional_edges(
        "MATCH_TWO_WAY",
        route_after_match,
        {
            "MATCHED": "RECONCILE",
            "FAILED": "CHECKPOINT_HITL",
        },
    )

    graph.add_conditional_edges(
        "HITL_DECISION",
        route_after_hitl,
        {
            "ACCEPT": "RECONCILE",
            "REJECT": "COMPLETE",
        },
    )

    graph.add_edge("RECONCILE", "APPROVE")
    graph.add_edge("APPROVE", "POSTING")
    graph.add_edge("POSTING", "NOTIFY")
    graph.add_edge("NOTIFY", "COMPLETE")
    graph.add_edge("COMPLETE", END)

    checkpointer = MemorySaver()  # in-memory LangGraph checkpoint
    app = graph.compile(checkpointer=checkpointer)
    return app


# -------------------------------------------------------------------
# 6. FASTAPI – HITL API CONTRACT
# -------------------------------------------------------------------

app = FastAPI(title="LangGraph Invoice Processing Agent")

# Allow all origins for local demo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


graph_main = build_graph(entry_node="INTAKE")
graph_from_hitl = build_graph(entry_node="HITL_DECISION")


class RunRequest(BaseModel):
    invoice_payload: Dict[str, Any]


class DecisionRequest(BaseModel):
    checkpoint_id: str
    decision: Literal["ACCEPT", "REJECT"]
    notes: Optional[str] = None
    reviewer_id: str


@app.post("/workflow/run")
def run_workflow(req: RunRequest):
    initial_state: WorkflowState = {
        "invoice_payload": req.invoice_payload,
        "logs": [],
        "status": "RUNNING",
    }

    # LangGraph with a checkpointer needs a thread_id in config
    thread_config = {"configurable": {"thread_id": str(uuid.uuid4())}}

    result: WorkflowState = graph_main.invoke(initial_state, config=thread_config)

    return {
        "status": result.get("status"),
        "match_score": result.get("match_score"),
        "match_result": result.get("match_result"),
        "checkpoint_id": result.get("hitl_checkpoint_id"),
        "review_url": result.get("review_url"),
        "final_payload": result.get("final_payload"),
        "logs": result.get("logs", []),
    }


@app.get("/human-review/pending")
def list_pending():
    return {"items": list_pending_checkpoints()}


@app.post("/human-review/decision")
def human_decision_api(req: DecisionRequest):
    try:
        state = get_checkpoint_state(req.checkpoint_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Checkpoint not found or resolved")

    state["human_decision"] = req.decision
    state["reviewer_id"] = req.reviewer_id
    log(state, f"HITL_API: Received human decision={req.decision} notes={req.notes}")

    # New thread_id for the resumed run
    thread_config = {"configurable": {"thread_id": str(uuid.uuid4())}}
    resumed: WorkflowState = graph_from_hitl.invoke(state, config=thread_config)

    mark_checkpoint_resolved(req.checkpoint_id)

    return {
        "resume_token": resumed.get("resume_token"),
        "next_stage": resumed.get("next_stage"),
        "status": resumed.get("status"),
        "final_payload": resumed.get("final_payload"),
        "logs": resumed.get("logs", []),
    }
