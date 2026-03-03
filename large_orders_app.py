"""
Large Orders — Live Dashboard  (reads from large_orders_data.csv)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import os

st.set_page_config(
    page_title="Large Orders — Live",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
    background:#0d0f16 !important; color:#e2e8f0 !important;
  }
  [data-testid="stSidebar"]       { background:#111320 !important; }
  .block-container                { padding-top:1rem !important; }

  /* metric cards */
  [data-testid="stMetric"]        { background:#111320; border:1px solid #1e2233;
                                    border-radius:10px; padding:14px 18px !important; }
  [data-testid="stMetricLabel"]   { color:#64748b !important; font-size:11px !important;
                                    text-transform:uppercase; letter-spacing:.5px; }
  [data-testid="stMetricValue"]   { color:#f1f5f9 !important; font-size:22px !important; font-weight:800; }

  /* tabs */
  [data-testid="stTabs"] button                    { color:#64748b !important; font-size:13px; }
  [data-testid="stTabs"] button[aria-selected="true"] { color:#818cf8 !important; border-bottom:2px solid #818cf8; }

  /* table */
  [data-testid="stDataFrame"]     { border:1px solid #1e2233; border-radius:8px; }

  /* inputs */
  [data-testid="stTextInput"] input { background:#111320 !important; border:1px solid #1e2233 !important;
                                       color:#e2e8f0 !important; border-radius:6px; }
  [data-testid="stSelectbox"] > div > div { background:#111320 !important;
                                             border:1px solid #1e2233 !important; color:#e2e8f0 !important; }

  /* buttons */
  [data-testid="stButton"] button { background:#1e2233; color:#e2e8f0;
                                     border:1px solid #2d3650; border-radius:7px; font-size:12px; }
  [data-testid="stButton"] button:hover { background:#2d3650; border-color:#818cf8; color:#818cf8; }

  /* live dot */
  .live-dot { display:inline-block; width:8px; height:8px; background:#22c55e;
              border-radius:50%; margin-right:6px; animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

  /* status pills */
  .pill      { display:inline-block; padding:3px 9px; border-radius:20px; font-size:11px; font-weight:600; }
  .p-hold    { background:rgba(239,68,68,.15);  color:#ef4444; }
  .p-pass    { background:rgba(34,197,94,.15);  color:#22c55e; }
  .p-prog    { background:rgba(245,158,11,.15); color:#f59e0b; }
  .p-appr    { background:rgba(34,197,94,.15);  color:#22c55e; }
  .p-none    { background:rgba(100,116,139,.1); color:#64748b; }

  /* detail panel */
  .det-grid  { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-top:4px; }
  .det-card  { background:#111320; border:1px solid #1e2233; border-radius:10px; padding:14px 16px; }
  .det-card h4 { font-size:10px; font-weight:700; color:#64748b; text-transform:uppercase;
                 letter-spacing:.8px; margin:0 0 10px; }
  .det-row   { display:flex; justify-content:space-between; align-items:baseline;
               padding:4px 0; border-bottom:1px solid #1a1f30; font-size:13px; }
  .det-row:last-child { border-bottom:none; }
  .det-lbl   { color:#64748b; min-width:110px; }
  .det-val   { color:#e2e8f0; font-weight:500; text-align:right; }
  a.qc-btn   { color:#818cf8; text-decoration:none; font-size:12px; padding:2px 0; }
  a.sl-btn   { color:#4a90e2; text-decoration:none; font-size:12px;
               background:rgba(74,144,226,.12); padding:3px 8px; border-radius:4px; }
  .badge-transit { background:rgba(6,182,212,.12);  color:#06b6d4; }
  .badge-qc      { background:rgba(129,140,248,.12); color:#818cf8; }
  .badge-new     { background:rgba(100,116,139,.1);  color:#94a3b8; }
  .badge-done    { background:rgba(34,197,94,.12);   color:#22c55e; }
  .badge-hold    { background:rgba(239,68,68,.12);   color:#ef4444; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
PAST_QC = {
    'QC_APPROVED','PICKUP_SUCCESSFULL','PICKUP_READY','PICKUP_REQUESTED',
    'PICKUP_SCHEDULED','FREIGHT','FREIGHT_DEPARTED','COURIER',
    'COURIER_CUSTOMS','HANDED_OVER_TO_LOGISTICS_PARTNER'
}

BASE_SLACK = "https://fleek-talk.slack.com/archives/C079ZJ9DSMT/"
CHAN       = "C079ZJ9DSMT"
SLACK_TS   = {
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

STATUS_BADGE = {
    'FREIGHT':'badge-transit ✈️ In Transit','FREIGHT_DEPARTED':'badge-transit ✈️ In Transit',
    'COURIER':'badge-transit ✈️ In Transit','COURIER_CUSTOMS':'badge-transit ✈️ In Transit',
    'HANDED_OVER_TO_LOGISTICS_PARTNER':'badge-transit ✈️ In Transit',
    'QC_PENDING':'badge-qc 🔍 At QC','QC_APPROVED':'badge-done ✅ QC Approved',
    'PICKUP_SUCCESSFULL':'badge-done ✅ QC Approved','PICKUP_READY':'badge-done 🤝 Handed Over',
    'PICKUP_REQUESTED':'badge-done 📦 Pickup','PICKUP_SCHEDULED':'badge-done 📦 Pickup',
    'ACCEPTED':'badge-new ✓ Accepted','CREATED':'badge-new 📋 New',
    'QC_HOLD':'badge-hold 🔴 QC Hold','PICKUP_FAILED':'badge-hold ⚠️ Failed',
}

def dash(v):
    """Return '—' for None/empty/nan values."""
    if v is None: return "—"
    s = str(v).strip()
    return "—" if s in ("", "None", "nan", "NaT", "none") else s

def slack_link(fid):
    ts = SLACK_TS.get(str(fid))
    if not ts: return None
    tts = ts[:10] + "." + ts[10:]
    return f"{BASE_SLACK}p{ts}?thread_ts={tts}&cid={CHAN}"

def effective_qc(row):
    r = row.get("qc_system_result", "")
    if r == "HOLD" and (row.get("qc_approved_date") or row.get("latest_status") in PAST_QC):
        return "APPROVED_FROM_HOLD"
    return r if r else "—"

def age_bucket(a):
    try: a = float(a)
    except: return "Unknown"
    if a <= 7:   return "0-7d"
    if a <= 14:  return "8-14d"
    if a <= 30:  return "15-30d"
    if a <= 60:  return "31-60d"
    if a <= 90:  return "61-90d"
    if a <= 180: return "91-180d"
    return "180+d"

def age_color(a):
    try: a = float(a)
    except: return "#64748b"
    if a <= 7:   return "#22c55e"
    if a <= 14:  return "#06b6d4"
    if a <= 30:  return "#f59e0b"
    if a <= 60:  return "#f97316"
    if a <= 90:  return "#ef4444"
    if a <= 180: return "#dc2626"
    return "#a855f7"

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_data():
    path = os.path.join(os.path.dirname(__file__), "large_orders_data.csv")
    df = pd.read_csv(path, dtype=str)          # load everything as strings first
    df = df.fillna("").replace("None","").replace("nan","").replace("NaT","")

    # Numeric columns
    for col in ["gmv_gbp","total_order_line_amount","orders_aging",
                "dispatched_box_count","dispatched_pieces","dispatched_weight_kg",
                "qc_system_total_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["slack_link"] = df["fleek_id"].apply(slack_link)

    # Ensure columns the detail panel references exist
    for col in ["targeted_delivery_date","ff_date","zone_mapping","self_ship",
                "rework_tag","ior","shipping_type"]:
        if col not in df.columns:
            df[col] = ""
    return df

# ── Header ────────────────────────────────────────────────────────────────────
h1, _, h3 = st.columns([5, 3, 1])
with h1:
    st.markdown('<span class="live-dot"></span>**Large Orders — Live Dashboard**', unsafe_allow_html=True)
    st.caption("GMV ≥ £2,000 · Open orders only · Data refreshed daily")
with h3:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

st.divider()

with st.spinner("Loading…"):
    df = load_data()

csv_path = os.path.join(os.path.dirname(__file__), "large_orders_data.csv")
if os.path.exists(csv_path):
    mtime     = os.path.getmtime(csv_path)
    refreshed = datetime.utcfromtimestamp(mtime).strftime("%d %b %Y %H:%M UTC")
else:
    refreshed = "unknown"

st.caption(f"📅 Data as of **{refreshed}**  ·  **{len(df):,}** open orders")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_ov, tab_all = st.tabs(["📊 Overview", "📋 All Orders"])

# ════════════════════════════════════════════════════════════════════════════
# OVERVIEW
# ════════════════════════════════════════════════════════════════════════════
with tab_ov:

    total_gmv  = df["gmv_gbp"].sum()
    hold_cnt   = (df["qc_system_result"] == "HOLD").sum()
    avg_age    = df["orders_aging"].mean()
    disp_boxes = df["dispatched_box_count"].sum()
    in_transit = df[df["latest_status"].isin(PAST_QC)].shape[0]
    at_qc      = (df["latest_status"] == "QC_PENDING").sum()

    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("Open Orders",  f"{len(df):,}")
    k2.metric("Total GMV",    f"£{total_gmv/1000:.0f}K")
    k3.metric("Avg Aging",    f"{avg_age:.0f}d")
    k4.metric("QC Hold",      f"{hold_cnt}")
    k5.metric("In Transit",   f"{in_transit}")
    k6.metric("At QC",        f"{at_qc}")

    st.markdown("---")

    col_a, col_b = st.columns([2, 1])

    with col_a:
        df["_age_bucket"] = df["orders_aging"].apply(age_bucket)
        bucket_order = ["0-7d","8-14d","15-30d","31-60d","61-90d","91-180d","180+d","Unknown"]
        color_map    = {"0-7d":"#22c55e","8-14d":"#06b6d4","15-30d":"#f59e0b",
                        "31-60d":"#f97316","61-90d":"#ef4444","91-180d":"#dc2626",
                        "180+d":"#a855f7","Unknown":"#475569"}
        age_df = (df.groupby("_age_bucket")
                    .agg(count=("fleek_id","count"), gmv=("gmv_gbp","sum"))
                    .reset_index())
        age_df["_age_bucket"] = pd.Categorical(age_df["_age_bucket"],
                                               categories=bucket_order, ordered=True)
        age_df = age_df.sort_values("_age_bucket").dropna(subset=["_age_bucket"])
        age_df["color"] = age_df["_age_bucket"].map(color_map)

        fig = go.Figure()
        fig.add_bar(
            x=age_df["_age_bucket"], y=age_df["count"],
            marker_color=age_df["color"],
            text=[f"{c}<br>£{g/1000:.0f}K" for c,g in zip(age_df["count"], age_df["gmv"])],
            textposition="outside", textfont=dict(size=10),
        )
        fig.update_layout(
            title=dict(text="Orders by Aging Bucket", font=dict(size=13)),
            paper_bgcolor="#0d0f16", plot_bgcolor="#0d0f16",
            font_color="#e2e8f0", height=290,
            margin=dict(t=44,b=20,l=10,r=10),
            xaxis=dict(gridcolor="#1a1f30", tickfont=dict(size=11)),
            yaxis=dict(gridcolor="#1a1f30"),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        pip = df["latest_status"].map(
            {k: v.split(" ",1)[1] for k,v in STATUS_BADGE.items()}
        ).fillna(df["latest_status"])
        pip_cnt = pip.value_counts().reset_index()
        pip_cnt.columns = ["status","count"]
        fig2 = px.pie(pip_cnt, values="count", names="status", hole=0.52,
                      color_discrete_sequence=["#818cf8","#06b6d4","#22c55e",
                                               "#f59e0b","#ef4444","#a855f7","#64748b"])
        fig2.update_traces(textfont_size=10)
        fig2.update_layout(
            title=dict(text="Status Mix", font=dict(size=13)),
            paper_bgcolor="#0d0f16", font_color="#e2e8f0",
            height=290, margin=dict(t=44,b=10,l=0,r=0),
            legend=dict(font=dict(size=10), orientation="v"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    col_c, col_d = st.columns([1, 2])

    with col_c:
        st.markdown("**Top Vendors by GMV**")
        vend_df = (df.groupby("vendor")
                     .agg(Orders=("fleek_id","count"), GMV=("gmv_gbp","sum"))
                     .sort_values("GMV", ascending=False).head(12).reset_index())
        vend_df["GMV"] = vend_df["GMV"].apply(lambda x: f"£{x:,.0f}")
        st.dataframe(vend_df, hide_index=True, use_container_width=True, height=300)

    with col_d:
        st.markdown("**Monthly GMV**")
        mon_df = (df.groupby("order_month")
                    .agg(GMV=("gmv_gbp","sum"), Orders=("fleek_id","count"))
                    .reset_index())
        fig3 = go.Figure()
        fig3.add_bar(
            x=mon_df["order_month"], y=mon_df["GMV"],
            marker_color="#818cf8",
            text=[f"£{v/1000:.0f}K\n{o} orders" for v,o in zip(mon_df["GMV"], mon_df["Orders"])],
            textposition="outside", textfont=dict(size=10),
        )
        fig3.update_layout(
            paper_bgcolor="#0d0f16", plot_bgcolor="#0d0f16",
            font_color="#e2e8f0", height=300,
            margin=dict(t=20,b=20,l=10,r=10),
            xaxis=dict(gridcolor="#1a1f30"), yaxis=dict(gridcolor="#1a1f30"),
            showlegend=False,
        )
        st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════
# ALL ORDERS
# ════════════════════════════════════════════════════════════════════════════
with tab_all:

    f1, f2, f3, f4 = st.columns([2.5, 1.5, 1.5, 1.5])
    search     = f1.text_input("🔍 Search Fleek ID, vendor, customer, tracking…",
                                placeholder="Search…", label_visibility="collapsed")
    status_opts = ["All Statuses"] + sorted(df["latest_status"].replace("","—").dropna().unique().tolist())
    qc_opts     = ["All QC","PASS","HOLD","IN_PROGRESS","APPROVED_FROM_HOLD","—"]
    month_opts  = ["All Months"]  + sorted(
        [m for m in df["order_month"].unique() if m], reverse=True)

    sel_status = f2.selectbox("Status", status_opts, label_visibility="collapsed")
    sel_qc     = f3.selectbox("QC",     qc_opts,     label_visibility="collapsed")
    sel_month  = f4.selectbox("Month",  month_opts,  label_visibility="collapsed")

    # Apply filters
    fdf = df.copy()
    fdf["_eff_qc"] = fdf.apply(effective_qc, axis=1)

    if search:
        s = search.lower()
        mask = (
            fdf["fleek_id"].str.lower().str.contains(s, na=False)
          | fdf["vendor"].str.lower().str.contains(s, na=False)
          | fdf["customer_name"].str.lower().str.contains(s, na=False)
          | fdf["tracking_number"].str.lower().str.contains(s, na=False)
        )
        fdf = fdf[mask]
    if sel_status != "All Statuses": fdf = fdf[fdf["latest_status"] == sel_status]
    if sel_month  != "All Months":   fdf = fdf[fdf["order_month"]   == sel_month]
    if sel_qc     != "All QC":       fdf = fdf[fdf["_eff_qc"]       == sel_qc]

    st.caption(f"**{len(fdf):,}** / {len(df):,} orders shown")

    # Build display table — replace empty with "—"
    disp = fdf[["fleek_id","vendor","customer_name","order_month","gmv_gbp","orders_aging",
                "customer_country","latest_status","qc_system_result",
                "dispatched_box_count","tracking_number","logistics_partner",
                "last_handover_date","_eff_qc"]].copy()

    disp.rename(columns={
        "fleek_id":"Fleek ID","vendor":"Vendor","customer_name":"Customer",
        "order_month":"Month","gmv_gbp":"GMV £","orders_aging":"Aging (d)",
        "customer_country":"Country","latest_status":"Status",
        "qc_system_result":"QC Raw","dispatched_box_count":"Boxes",
        "tracking_number":"Tracking","logistics_partner":"Partner",
        "last_handover_date":"Last Handover","_eff_qc":"QC Status",
    }, inplace=True)

    disp["GMV £"]       = disp["GMV £"].apply(lambda v: f"£{v:,.0f}" if pd.notna(v) else "—")
    disp["Aging (d)"]   = disp["Aging (d)"].apply(lambda v: f"{v:.0f}" if pd.notna(v) else "—")
    disp["Boxes"]       = disp["Boxes"].apply(lambda v: f"{int(v)}" if pd.notna(v) else "—")

    # Replace empty strings and NaN with "—" in string columns
    str_cols = ["Vendor","Customer","Country","Status","QC Raw","Tracking",
                "Partner","Last Handover","QC Status"]
    for c in str_cols:
        disp[c] = disp[c].replace({"": "—", "nan": "—", "None": "—"}).fillna("—")

    st.dataframe(
        disp,
        hide_index=True,
        use_container_width=True,
        height=380,
        column_config={
            "Fleek ID":    st.column_config.TextColumn(width="small"),
            "GMV £":       st.column_config.TextColumn(width="small"),
            "Aging (d)":   st.column_config.TextColumn(width="small"),
            "Boxes":       st.column_config.TextColumn(width="small"),
            "QC Status":   st.column_config.TextColumn(width="small"),
            "QC Raw":      st.column_config.TextColumn(width="small"),
        }
    )

    # ── Detail panel ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🔍 Order Detail")
    st.caption("Select any order above, or search by Fleek ID:")

    sel_id = st.selectbox("Fleek ID", ["—"] + fdf["fleek_id"].tolist(),
                          label_visibility="collapsed")

    if sel_id != "—":
        row    = fdf[fdf["fleek_id"] == sel_id].iloc[0].to_dict()
        eq     = effective_qc(row)
        sl     = slack_link(sel_id)
        qc_url = (
            f"https://shop.joinfleek.com/qc/{row.get('order_line_id')}"
            f"?source=QCAPP&qcProductId={row.get('qc_product_id')}"
            if row.get("order_line_id") and row.get("qc_product_id") else None
        )

        def dr(lbl, val):
            v = dash(val)
            return (f'<div class="det-row">'
                    f'<span class="det-lbl">{lbl}</span>'
                    f'<span class="det-val">{v}</span></div>')

        links = ""
        if qc_url: links += f'&nbsp;<a class="qc-btn" href="{qc_url}" target="_blank">🔗 QC Report</a>'
        if sl:     links += f'&nbsp;<a class="sl-btn" href="{sl}"     target="_blank">💬 Slack</a>'

        gmv_fmt = f"£{float(row.get('gmv_gbp',0)):,.0f}" if row.get('gmv_gbp') else "—"

        pieces_fmt = "—"
        if row.get("dispatched_pieces") and str(row["dispatched_pieces"]) not in ("","nan","None"):
            try: pieces_fmt = f"{float(row['dispatched_pieces']):,.0f}"
            except: pass

        weight_fmt = "—"
        if row.get("dispatched_weight_kg") and str(row["dispatched_weight_kg"]) not in ("","nan","None"):
            try: weight_fmt = f"{float(row['dispatched_weight_kg']):.2f} kg"
            except: pass

        html = f"""
        <div class="det-grid">
          <div class="det-card">
            <h4>📋 Order Info</h4>
            {dr('Fleek ID',    row.get('fleek_id'))}
            {dr('Vendor',      row.get('vendor'))}
            {dr('Customer',    row.get('customer_name'))}
            {dr('Country',     row.get('customer_country'))}
            {dr('GMV',         gmv_fmt)}
            {dr('Order Date',  row.get('order_date'))}
            {dr('Month',       row.get('order_month'))}
            {dr('Aging',       f"{row.get('orders_aging',0):.0f}d" if row.get('orders_aging') else '—')}
          </div>
          <div class="det-card">
            <h4>🚀 Fulfillment</h4>
            {dr('Status',        row.get('latest_status'))}
            {dr('Handover Date', row.get('handover_date'))}
            {dr('OMS Boxes',     row.get('boxes'))}
            {dr('Ship Mode',     row.get('shipping_mode'))}
            {dr('Went to Hold',  row.get('went_to_qc_hold'))}
            {dr('QC Pending',    row.get('qc_pending_date'))}
            {dr('QC Hold Date',  row.get('qc_hold_date'))}
            {dr('QC Approved',   row.get('qc_approved_date'))}
          </div>
          <div class="det-card">
            <h4>✈️ Logistics</h4>
            {dr('Tracking #',    row.get('tracking_number'))}
            {dr('Partner',       row.get('logistics_partner'))}
            {dr('Flight #',      row.get('flight_number'))}
            {dr('Last Handover', row.get('last_handover_date'))}
            {dr('Disp. Boxes',   row.get('dispatched_box_count'))}
            {dr('Pieces',        pieces_fmt)}
            {dr('Weight',        weight_fmt)}
            {dr('Box Sizes',     row.get('box_sizes'))}
          </div>
          <div class="det-card">
            <h4>🔍 QC &nbsp;{links}</h4>
            {dr('Result',       eq.replace('APPROVED_FROM_HOLD','✅ Approved (was Hold)'))}
            {dr('Reason',       row.get('qc_system_reason'))}
            {dr('Title',        row.get('qc_system_title'))}
            {dr('Count',        row.get('qc_system_total_count'))}
            {dr('QC Date',      row.get('qc_system_date'))}
            {dr('Last Updated', row.get('qc_system_updated'))}
          </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
