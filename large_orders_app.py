"""
Large Orders — Live Dashboard
Run with: streamlit run large_orders_app.py
Requires: pip install streamlit plotly google-cloud-bigquery pandas
Auth:      gcloud auth application-default login
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json, re

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Large Orders — Live",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Dark theme CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* global dark */
  html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
    background:#0d0f16 !important; color:#e2e8f0 !important;
  }
  [data-testid="stSidebar"] { background:#111320 !important; }
  .block-container { padding-top:1rem !important; }

  /* metric cards */
  [data-testid="stMetric"] {
    background:#111320; border:1px solid #1e2233; border-radius:10px;
    padding:14px 18px !important;
  }
  [data-testid="stMetricLabel"] { color:#64748b !important; font-size:11px !important; text-transform:uppercase; letter-spacing:.5px; }
  [data-testid="stMetricValue"] { color:#f1f5f9 !important; font-size:22px !important; font-weight:800; }
  [data-testid="stMetricDelta"] { font-size:11px !important; }

  /* tabs */
  [data-testid="stTabs"] button { color:#64748b !important; font-size:13px; }
  [data-testid="stTabs"] button[aria-selected="true"] { color:#818cf8 !important; border-bottom:2px solid #818cf8; }

  /* dataframe */
  [data-testid="stDataFrame"] { border:1px solid #1e2233; border-radius:8px; }

  /* expander */
  [data-testid="stExpander"] { background:#111320 !important; border:1px solid #1e2233 !important; border-radius:8px; }
  [data-testid="stExpanderDetails"] { background:#0d0f16 !important; }

  /* buttons */
  [data-testid="stButton"] button {
    background:#1e2233; color:#e2e8f0; border:1px solid #2d3650;
    border-radius:7px; font-size:12px;
  }
  [data-testid="stButton"] button:hover { background:#2d3650; border-color:#818cf8; color:#818cf8; }

  /* selectbox / filter */
  [data-testid="stSelectbox"] > div > div { background:#111320 !important; border:1px solid #1e2233 !important; color:#e2e8f0 !important; }

  /* header bar */
  .dash-header {
    background:#111320; border-bottom:1px solid #1e2233;
    padding:12px 0 10px; margin-bottom:18px;
    display:flex; align-items:center; justify-content:space-between;
  }
  .dash-title { font-size:18px; font-weight:800; color:#f1f5f9; }
  .dash-sub   { font-size:11px; color:#475569; margin-top:3px; }
  .live-dot   { display:inline-block; width:8px; height:8px; background:#22c55e;
                border-radius:50%; margin-right:5px; animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }

  /* pills */
  .pill { display:inline-block; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:600; }
  .pill-hold { background:rgba(239,68,68,.15); color:#ef4444; }
  .pill-pass { background:rgba(34,197,94,.15); color:#22c55e; }
  .pill-prog { background:rgba(245,158,11,.15); color:#f59e0b; }
  .pill-none { background:rgba(100,116,139,.1); color:#64748b; }
  .pill-appr { background:rgba(34,197,94,.15); color:#22c55e; }

  /* detail grid */
  .detail-grid { display:grid; grid-template-columns:1fr 1fr; gap:18px; }
  .detail-section { background:#111320; border-radius:8px; padding:14px 16px; }
  .detail-section h4 { font-size:11px; font-weight:700; color:#64748b; text-transform:uppercase;
                       letter-spacing:.7px; margin:0 0 10px; }
  .detail-row { display:flex; justify-content:space-between; margin-bottom:7px; font-size:13px; }
  .detail-label { color:#64748b; }
  .detail-val   { color:#e2e8f0; font-weight:500; }
  a.qc-link  { color:#818cf8; text-decoration:none; font-size:12px; }
  a.slk-link { color:#4a90e2; text-decoration:none; font-size:12px; background:rgba(74,144,226,.1);
               padding:2px 8px; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

PROJECT = "dogwood-baton-345622"

# ── BigQuery SQL ──────────────────────────────────────────────────────────────
SQL = """
WITH base AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_line_id ORDER BY targetted_delivery_date DESC NULLS LAST) AS rn_oms
  FROM `dogwood-baton-345622.fleek_raw.order_line_status_details`
  WHERE total_order_line_amount * 1.3 >= 2000
    AND created_at >= '2025-07-01'
    AND latest_status NOT IN ('DELIVERED','CANCELLED')
    AND delivered_at IS NULL
    AND vendor NOT IN ('alexprodshop','ranilists','azher-ratnani','walitest')
    AND fleek_id NOT IN ('2023-0002672','2023-0002671','2023-0002670','2024-0001699')
),
deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY fleek_id ORDER BY total_order_line_amount DESC, order_line_id) AS rn_fid
  FROM base WHERE rn_oms = 1
),
qc_ranked AS (
  SELECT id AS qc_product_id, line_item_id,
         result, reason, action_taken, rework_type, source, title,
         total_count, created_at AS qc_date, updated_at AS qc_updated,
         ROW_NUMBER() OVER (PARTITION BY line_item_id ORDER BY updated_at DESC) AS rn
  FROM `dogwood-baton-345622.aurora_postgres_public.qc_product`
  WHERE _fivetran_deleted = FALSE
),
boxes AS (
  SELECT REPLACE(internal_order_id,'/',  '_') AS fleek_id,
         COUNT(DISTINCT id) AS box_count,
         SUM(pieces) AS pieces,
         ROUND(SUM(weight_grams)/1000.0,2) AS weight_kg,
         MAX(DATE(handover_at)) AS last_handover,
         STRING_AGG(DISTINCT box_size, ', ') AS box_sizes
  FROM `dogwood-baton-345622.aurora_postgres_public.line_item_boxes`
  WHERE _fivetran_deleted = FALSE AND internal_order_id IS NOT NULL
  GROUP BY 1
)
SELECT
  d.fleek_id,
  d.order_line_id,
  d.vendor,
  d.customer_name,
  d.customer_country,
  d.total_order_line_amount,
  ROUND(d.total_order_line_amount * 1.3, 0) AS gmv_gbp,
  d.latest_status,
  d.created_at,
  DATE(d.created_at) AS order_date,
  FORMAT_DATE('%b %Y', DATE(d.created_at)) AS order_month,
  DATE_DIFF(CURRENT_DATE(), DATE(d.created_at), DAY) AS orders_aging,
  d.targetted_delivery_date AS targeted_delivery_date,
  d.ff_date,
  d.handover_date,
  d.tracking_number,
  d.logistics_partner,
  d.flight_number,
  d.qc_pending_date,
  d.qc_approved_date,
  d.qc_hold_date,
  d.went_to_qc_hold,
  d.zone_mapping,
  d.self_ship,
  d.rework_tag,
  d.boxes,
  d.ior,
  d.shipping_mode,
  d.shipping_type,
  -- QC system
  q.qc_product_id,
  q.result   AS qc_system_result,
  q.reason   AS qc_system_reason,
  q.title    AS qc_system_title,
  q.total_count AS qc_system_total_count,
  q.qc_date  AS qc_system_date,
  q.qc_updated AS qc_system_updated,
  -- Boxes
  b.box_count AS dispatched_box_count,
  b.pieces    AS dispatched_pieces,
  b.weight_kg AS dispatched_weight_kg,
  b.last_handover AS last_handover_date,
  b.box_sizes
FROM deduped d
LEFT JOIN qc_ranked q ON CAST(d.order_line_id AS STRING) = q.line_item_id AND q.rn = 1
LEFT JOIN boxes b ON b.fleek_id = d.fleek_id
WHERE d.rn_fid = 1
ORDER BY d.created_at DESC
"""

# ── Slack QC hold links ───────────────────────────────────────────────────────
BASE_SLACK = "https://fleek-talk.slack.com/archives/C079ZJ9DSMT/"
CHAN = "C079ZJ9DSMT"
SLACK_TS = {
    "129774_06":"1772515837787009","130724_50":"1772513322118499","131066_70":"1772512276822119",
    "128991_18":"1772451990004469","131527_42":"1772449273969749","131069_18":"1772442274359869",
    "131952_42":"1772438726253839","131403_50":"1772435338276419","130425_50":"1772255920222239",
    "127462_18":"1772254053438809","129854_66":"1772163039722299","129922_66":"1772097764275279",
    "130443_58":"1772086083748089","128291_14":"1771996699305589","128647_74":"1771994884023219",
    "127972_58":"1771993524652779","128057_50":"1771990226998349","129082_30":"1771649578738529",
    "128416_10":"1771646609475579","128754_86":"1771645678571719","128817_46":"1771644867765679",
    "127985_94":"1771492849510379","124284_58":"1771487469875419","128499_86":"1771411535109689",
    "128134_34":"1771327357086659","128134_66":"1771325185083069","126911_26":"1771324031167419",
    "122465_62":"1771315002693729","124149_74":"1771314344179639","124149_06":"1771227364186019",
    "104768_10":"1771225967400859","127518_86":"1771091653436239","127608_74":"1771064532757739",
    "123262_50":"1771056146282629","126756_70":"1770986956360539","126383_14":"1770984657415579",
    "124372_46":"1770974225259529","124199_86":"1770969465552669","126425_22":"1770966453164999",
    "104768_78":"1770907893076969","121416_58":"1770898331489429","125888_10":"1770883661177309",
    "104768_82":"1770881670253169","125156_18":"1770800661505979","126054_58":"1770720809623829",
    "125912_38":"1770719277240019","124154_62":"1770714808179149","121815_62":"1770713547373059",
    "123764_78":"1770644654501869","125179_78":"1770640339415929","124569_90":"1770632623636799",
    "123465_50":"1770456471550979","123434_30":"1770450980012919","115435_38":"1770449250908589",
    "121943_98":"1770387871248769","121435_70":"1770373212266439","119006_14":"1770369830867799",
    "123997_46":"1770291166035799","121345_06":"1770123134904639","123328_14":"1770102793036919",
}
def slack_link(fleek_id):
    ts = SLACK_TS.get(fleek_id)
    if not ts: return None
    thread_ts = ts[:10] + "." + ts[10:]
    return f"{BASE_SLACK}p{ts}?thread_ts={thread_ts}&cid={CHAN}"

# ── Helpers ───────────────────────────────────────────────────────────────────
PAST_QC = {'QC_APPROVED','PICKUP_SUCCESSFULL','PICKUP_READY','PICKUP_REQUESTED',
           'PICKUP_SCHEDULED','FREIGHT','FREIGHT_DEPARTED','COURIER',
           'COURIER_CUSTOMS','HANDED_OVER_TO_LOGISTICS_PARTNER'}

def effective_qc(row):
    r = row.get("qc_system_result")
    if r == "HOLD" and (row.get("qc_approved_date") or row.get("latest_status") in PAST_QC):
        return "APPROVED_FROM_HOLD"
    return r or "—"

def qc_pill(result):
    m = {"PASS":"pill-pass ✅ Pass","HOLD":"pill-hold 🔴 Hold",
         "APPROVED_FROM_HOLD":"pill-appr ✅ Approved","IN_PROGRESS":"pill-prog ⏳ In Prog"}
    parts = m.get(result, f"pill-none {result}").split(" ", 1)
    cls, label = parts[0], parts[1] if len(parts)>1 else result
    return f'<span class="pill {cls}">{label}</span>'

def age_color(a):
    if a<=7: return "#22c55e"
    if a<=14: return "#06b6d4"
    if a<=30: return "#f59e0b"
    if a<=60: return "#f97316"
    if a<=90: return "#ef4444"
    if a<=180: return "#dc2626"
    return "#a855f7"

def age_bucket(a):
    if a<=7: return "0-7d"
    if a<=14: return "8-14d"
    if a<=30: return "15-30d"
    if a<=60: return "31-60d"
    if a<=90: return "61-90d"
    if a<=180: return "91-180d"
    return "180+d"

STATUS_MAP = {
    'FREIGHT':'✈️ In Transit','FREIGHT_DEPARTED':'✈️ In Transit',
    'COURIER':'✈️ In Transit','COURIER_CUSTOMS':'✈️ In Transit',
    'HANDED_OVER_TO_LOGISTICS_PARTNER':'✈️ In Transit',
    'ACCEPTED':'✓ Accepted','CREATED':'📋 New Order',
    'QC_PENDING':'🔍 At QC','QC_APPROVED':'✅ QC Approved',
    'PICKUP_SUCCESSFULL':'✅ QC Approved','PICKUP_READY':'🤝 Handed Over',
    'PICKUP_REQUESTED':'📦 Pickup','PICKUP_SCHEDULED':'📦 Pickup',
    'PICKUP_FAILED':'⚠️ Pickup Failed',
}

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=7200, show_spinner=False)
def load_data():
    try:
        from google.cloud import bigquery
        # Cloud deployment: use service account from st.secrets
        # Local development: fall back to Application Default Credentials
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(project=PROJECT, credentials=creds)
        else:
            client = bigquery.Client(project=PROJECT)
        df = client.query(SQL).to_dataframe()
        df["slack_link"] = df["fleek_id"].apply(slack_link)
        return df, None
    except Exception as e:
        return None, str(e)

# ── Main App ──────────────────────────────────────────────────────────────────
col_h1, col_h2, col_h3 = st.columns([4,2,1])
with col_h1:
    st.markdown('<span class="live-dot"></span> **Large Orders — Live Dashboard**', unsafe_allow_html=True)
    st.caption("GMV ≥ £2,000 · Open orders · Auto-refreshes every 2h")
with col_h3:
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()

st.divider()

# Load
with st.spinner("Loading orders from BigQuery…"):
    df, err = load_data()

if err:
    st.error(f"BigQuery connection failed: {err}")
    st.info("Run `gcloud auth application-default login` in your terminal, then refresh.")
    st.stop()

now = datetime.now().strftime("%d %b %Y %H:%M")
st.caption(f"Last pulled: {now} · {len(df):,} open orders")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_ov, tab_all = st.tabs(["📊 Overview", "📋 All Orders"])

# ════════════════════════════════════════════════════════════════════════════
# OVERVIEW TAB
# ════════════════════════════════════════════════════════════════════════════
with tab_ov:

    # KPI row
    total_gmv  = df["gmv_gbp"].sum()
    total_ord  = len(df)
    hold_cnt   = df[df["qc_system_result"]=="HOLD"].shape[0]
    avg_age    = df["orders_aging"].mean()
    disp_boxes = df["dispatched_box_count"].sum()
    in_transit = df[df["latest_status"].isin(PAST_QC)].shape[0]

    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("Open Orders", f"{total_ord:,}")
    k2.metric("Total GMV", f"£{total_gmv/1000:.0f}K")
    k3.metric("Avg Aging", f"{avg_age:.0f}d")
    k4.metric("QC Hold", f"{hold_cnt}")
    k5.metric("In Transit", f"{in_transit}")
    k6.metric("Disp. Boxes", f"{int(disp_boxes):,}" if disp_boxes else "0")

    st.markdown("---")

    # Row 2: Aging cards + pipeline
    c1, c2 = st.columns([2,1])

    with c1:
        # Aging breakdown bar chart
        df["age_bucket"] = df["orders_aging"].apply(age_bucket)
        bucket_order = ["0-7d","8-14d","15-30d","31-60d","61-90d","91-180d","180+d"]
        colors_map = {"0-7d":"#22c55e","8-14d":"#06b6d4","15-30d":"#f59e0b",
                      "31-60d":"#f97316","61-90d":"#ef4444","91-180d":"#dc2626","180+d":"#a855f7"}
        age_df = df.groupby("age_bucket").agg(count=("fleek_id","count"), gmv=("gmv_gbp","sum")).reset_index()
        age_df["age_bucket"] = pd.Categorical(age_df["age_bucket"], categories=bucket_order, ordered=True)
        age_df = age_df.sort_values("age_bucket")
        age_df["color"] = age_df["age_bucket"].map(colors_map)

        fig_age = go.Figure()
        fig_age.add_bar(x=age_df["age_bucket"], y=age_df["count"],
                        marker_color=age_df["color"], name="Orders",
                        text=age_df["count"], textposition="outside")
        fig_age.update_layout(
            title="Orders by Aging Bucket", paper_bgcolor="#0d0f16", plot_bgcolor="#0d0f16",
            font_color="#e2e8f0", height=280, margin=dict(t=40,b=20,l=20,r=20),
            xaxis=dict(gridcolor="#1e2233"), yaxis=dict(gridcolor="#1e2233"),
            showlegend=False,
        )
        st.plotly_chart(fig_age, use_container_width=True)

    with c2:
        # Pipeline donut
        pip_df = df["latest_status"].map(STATUS_MAP).fillna(df["latest_status"])
        pip_counts = pip_df.value_counts().reset_index()
        pip_counts.columns = ["status","count"]
        fig_pip = px.pie(pip_counts, values="count", names="status", hole=0.55,
                         color_discrete_sequence=px.colors.qualitative.Set3)
        fig_pip.update_layout(
            title="Status Pipeline", paper_bgcolor="#0d0f16",
            font_color="#e2e8f0", height=280, margin=dict(t=40,b=10,l=0,r=0),
            legend=dict(font=dict(size=10)),
        )
        st.plotly_chart(fig_pip, use_container_width=True)

    # Row 3: Zone table + Monthly GMV
    c3, c4 = st.columns([1,2])
    with c3:
        st.markdown("**Zone Breakdown**")
        zone_df = df.groupby("zone_mapping").agg(
            Orders=("fleek_id","count"), GMV=("gmv_gbp","sum")
        ).sort_values("GMV", ascending=False).reset_index()
        zone_df["GMV"] = zone_df["GMV"].apply(lambda x: f"£{x:,.0f}")
        st.dataframe(zone_df, hide_index=True, use_container_width=True,
                     height=220)

    with c4:
        st.markdown("**Monthly GMV**")
        mon_df = df.groupby("order_month").agg(
            GMV=("gmv_gbp","sum"), Orders=("fleek_id","count")
        ).reset_index()
        fig_mon = go.Figure()
        fig_mon.add_bar(x=mon_df["order_month"], y=mon_df["GMV"],
                        marker_color="#818cf8", name="GMV",
                        text=mon_df["GMV"].apply(lambda v:f"£{v/1000:.0f}K"),
                        textposition="outside")
        fig_mon.update_layout(
            paper_bgcolor="#0d0f16", plot_bgcolor="#0d0f16",
            font_color="#e2e8f0", height=220, margin=dict(t=20,b=20,l=20,r=20),
            xaxis=dict(gridcolor="#1e2233"), yaxis=dict(gridcolor="#1e2233"),
            showlegend=False,
        )
        st.plotly_chart(fig_mon, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════
# ALL ORDERS TAB
# ════════════════════════════════════════════════════════════════════════════
with tab_all:

    # Filter row
    f1,f2,f3,f4,f5 = st.columns([2,1.5,1.2,1.2,1.2])
    search = f1.text_input("🔍 Search fleek ID, vendor…", placeholder="", label_visibility="collapsed")
    status_opts = ["All Statuses"] + sorted(df["latest_status"].dropna().unique().tolist())
    zone_opts   = ["All Zones"]   + sorted(df["zone_mapping"].dropna().unique().tolist())
    qc_opts     = ["All QC","PASS","HOLD","IN_PROGRESS","APPROVED_FROM_HOLD","—"]
    month_opts  = ["All Months"]  + sorted(df["order_month"].dropna().unique().tolist(), reverse=True)

    sel_status = f2.selectbox("Status",  status_opts, label_visibility="collapsed")
    sel_zone   = f3.selectbox("Zone",    zone_opts,   label_visibility="collapsed")
    sel_qc     = f4.selectbox("QC",      qc_opts,     label_visibility="collapsed")
    sel_month  = f5.selectbox("Month",   month_opts,  label_visibility="collapsed")

    # Apply filters
    fdf = df.copy()
    fdf["_eff_qc"] = fdf.apply(effective_qc, axis=1)
    if search:
        mask = fdf.apply(lambda r: search.lower() in str(r.get("fleek_id","")).lower()
                         or search.lower() in str(r.get("vendor","")).lower()
                         or search.lower() in str(r.get("tracking_number","")).lower(), axis=1)
        fdf = fdf[mask]
    if sel_status != "All Statuses": fdf = fdf[fdf["latest_status"]==sel_status]
    if sel_zone   != "All Zones":    fdf = fdf[fdf["zone_mapping"]==sel_zone]
    if sel_month  != "All Months":   fdf = fdf[fdf["order_month"]==sel_month]
    if sel_qc     != "All QC":       fdf = fdf[fdf["_eff_qc"]==sel_qc]

    st.caption(f"{len(fdf):,} / {len(df):,} orders")

    # Table columns to display
    display_cols = ["fleek_id","vendor","order_month","gmv_gbp","orders_aging",
                    "zone_mapping","latest_status","targeted_delivery_date",
                    "dispatched_box_count","dispatched_pieces","last_handover_date",
                    "tracking_number","logistics_partner","_eff_qc"]
    col_labels = {
        "fleek_id":"Fleek ID","vendor":"Vendor","order_month":"Month",
        "gmv_gbp":"GMV £","orders_aging":"Aging (d)","zone_mapping":"Zone",
        "latest_status":"Status","targeted_delivery_date":"Delivery Date",
        "dispatched_box_count":"Disp.Boxes","dispatched_pieces":"Pieces",
        "last_handover_date":"Last Handover","tracking_number":"Tracking #",
        "logistics_partner":"Partner","_eff_qc":"QC",
    }
    tdf = fdf[display_cols].rename(columns=col_labels).copy()
    tdf["GMV £"] = tdf["GMV £"].apply(lambda v: f"£{v:,.0f}" if pd.notna(v) else "—")

    # Render table
    st.dataframe(
        tdf,
        hide_index=True,
        use_container_width=True,
        height=350,
        column_config={
            "Fleek ID": st.column_config.TextColumn(width="small"),
            "GMV £":    st.column_config.TextColumn(width="small"),
            "Aging (d)":st.column_config.NumberColumn(width="small", format="%dd"),
        }
    )

    st.markdown("---")
    st.markdown("#### 🔍 Order Detail")
    st.caption("Select an order from the table above or search by Fleek ID:")

    sel_id = st.selectbox("Fleek ID", ["—"] + fdf["fleek_id"].tolist(), label_visibility="collapsed")

    if sel_id != "—":
        row = fdf[fdf["fleek_id"]==sel_id].iloc[0].to_dict()
        eq = effective_qc(row)
        sl = slack_link(sel_id)
        qc_url = (f"https://shop.joinfleek.com/qc/{row.get('order_line_id')}"
                  f"?source=QCAPP&qcProductId={row.get('qc_product_id')}"
                  if row.get("order_line_id") and row.get("qc_product_id") else None)

        def dr(label, val):
            v = val if val and str(val) not in ("None","nan","NaT","") else "—"
            return f'<div class="detail-row"><span class="detail-label">{label}</span><span class="detail-val">{v}</span></div>'

        links_html = ""
        if qc_url:
            links_html += f' &nbsp;<a class="qc-link" href="{qc_url}" target="_blank">🔗 QC Report</a>'
        if sl:
            links_html += f' &nbsp;<a class="slk-link" href="{sl}" target="_blank">💬 Slack</a>'

        html = f"""
        <div class="detail-grid">
          <div class="detail-section">
            <h4>📋 Order</h4>
            {dr('Fleek ID', row.get('fleek_id'))}
            {dr('Vendor', row.get('vendor'))}
            {dr('Customer', row.get('customer_name'))}
            {dr('Country', row.get('customer_country'))}
            {dr('GMV', f"£{row.get('gmv_gbp',0):,.0f}")}
            {dr('Zone', row.get('zone_mapping'))}
            {dr('Order Date', row.get('order_date'))}
            {dr('Aging', f"{row.get('orders_aging',0)}d")}
            {dr('Delivery Date', row.get('targeted_delivery_date'))}
          </div>
          <div class="detail-section">
            <h4>🚀 Fulfillment</h4>
            {dr('Status', row.get('latest_status'))}
            {dr('FF Date', row.get('ff_date'))}
            {dr('Handover Date', row.get('handover_date'))}
            {dr('Self Ship', row.get('self_ship'))}
            {dr('Rework Tag', row.get('rework_tag'))}
            {dr('OMS Boxes', row.get('boxes'))}
            {dr('IOR', row.get('ior'))}
            {dr('Ship Mode', row.get('shipping_mode'))}
            {dr('Ship Type', row.get('shipping_type'))}
          </div>
          <div class="detail-section">
            <h4>✈️ Logistics</h4>
            {dr('Tracking #', row.get('tracking_number'))}
            {dr('Partner', row.get('logistics_partner'))}
            {dr('Flight #', row.get('flight_number'))}
            {dr('Last Handover', row.get('last_handover_date'))}
            {dr('Disp. Boxes', row.get('dispatched_box_count'))}
            {dr('Pieces', f"{row.get('dispatched_pieces',0):,.0f}" if row.get('dispatched_pieces') else '—')}
            {dr('Weight', f"{row.get('dispatched_weight_kg')}kg" if row.get('dispatched_weight_kg') else '—')}
            {dr('Box Sizes', row.get('box_sizes'))}
          </div>
          <div class="detail-section">
            <h4>🔍 QC {links_html}</h4>
            {dr('System Result', eq.replace('APPROVED_FROM_HOLD','✅ Approved (was Hold)'))}
            {dr('Reason', row.get('qc_system_reason'))}
            {dr('Title', row.get('qc_system_title'))}
            {dr('Total Count', row.get('qc_system_total_count'))}
            {dr('QC Pending', row.get('qc_pending_date'))}
            {dr('QC Hold', row.get('qc_hold_date'))}
            {dr('QC Approved', row.get('qc_approved_date'))}
            {dr('Last Updated', row.get('qc_system_updated'))}
          </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
