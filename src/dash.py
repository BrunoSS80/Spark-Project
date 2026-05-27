import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sales Dashboard",
    page_icon="📦",
    layout="wide"
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Sans:wght@300;400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        background-color: #0d0d0d;
        color: #f0f0f0;
    }

    .main { background-color: #0d0d0d; }

    h1, h2, h3 {
        font-family: 'Syne', sans-serif;
        font-weight: 800;
        letter-spacing: -1px;
    }

    .metric-card {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
    }

    .metric-label {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: #888;
        margin-bottom: 6px;
    }

    .metric-value {
        font-family: 'Syne', sans-serif;
        font-size: 28px;
        font-weight: 800;
        color: #f0f0f0;
    }

    .metric-value.accent { color: #00e5a0; }

    .section-title {
        font-family: 'Syne', sans-serif;
        font-size: 13px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 3px;
        color: #555;
        margin-bottom: 12px;
    }

    .stSelectbox label, .stMultiSelect label {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #666 !important;
    }

    [data-testid="stSidebar"] {
        background-color: #111 !important;
        border-right: 1px solid #222;
    }

    .block-container { padding-top: 2rem; }
</style>
""", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    df = pd.read_parquet("/opt/spark-data/gold/master_sales.parquet")
    df["order_purchase_date"] = pd.to_datetime(df["order_purchase_date"])
    df["year"] = df["order_purchase_date"].dt.year
    df["month"] = df["order_purchase_date"].dt.month
    df["month_name"] = df["order_purchase_date"].dt.strftime("%b")
    return df

df = load_data()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📦 Filtros")
    st.markdown("---")

    selected_status = st.multiselect(
        "Status do Pedido",
        options=sorted(df["order_status"].dropna().unique()),
        default=[]
    )

    selected_payment = st.multiselect(
        "Tipo de Pagamento",
        options=sorted(df["principal_payment_type"].dropna().unique()),
        default=[]
    )

# ── Apply filters ─────────────────────────────────────────────────────────────
dff = df.copy()
if selected_status:
    dff = dff[dff["order_status"].isin(selected_status)]
if selected_payment:
    dff = dff[dff["principal_payment_type"].isin(selected_payment)]

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("# Sales Dashboard")
st.markdown("---")

# ── KPI cards ─────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)

total_revenue = dff["value_items"].sum()
total_orders  = dff["order_id"].nunique()
total_freight = dff["total_freight"].sum()
avg_ticket    = total_revenue / total_orders if total_orders > 0 else 0

with k1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Receita Total</div>
        <div class="metric-value accent">R$ {total_revenue:,.0f}</div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Total de Pedidos</div>
        <div class="metric-value">{total_orders:,}</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Total Frete</div>
        <div class="metric-value">R$ {total_freight:,.0f}</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Ticket Médio</div>
        <div class="metric-value">R$ {avg_ticket:,.0f}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Main chart — Top 5 estados ────────────────────────────────────────────────
st.markdown('<p class="section-title">Top 5 Estados por Vendas</p>', unsafe_allow_html=True)

state_df = (
    dff.groupby("customer_state")["value_items"]
    .sum()
    .reset_index()
    .sort_values("value_items", ascending=False)
    .head(5)
)

fig_states = px.bar(
    state_df,
    x="customer_state",
    y="value_items",
    color="value_items",
    color_continuous_scale=["#1a1a2e", "#00e5a0"],
    labels={"customer_state": "Estado", "value_items": "Receita (R$)"},
)

fig_states.update_layout(
    plot_bgcolor="#0d0d0d",
    paper_bgcolor="#0d0d0d",
    font=dict(color="#f0f0f0", family="DM Sans"),
    coloraxis_showscale=False,
    xaxis=dict(showgrid=False, tickfont=dict(size=11)),
    yaxis=dict(showgrid=True, gridcolor="#1f1f1f", tickprefix="R$ "),
    margin=dict(l=0, r=0, t=10, b=0),
    height=340,
)
fig_states.update_traces(marker_line_width=0)

st.plotly_chart(fig_states, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Bottom charts ─────────────────────────────────────────────────────────────
col_left, col_right = st.columns([1, 1])

# ── Left — bucket de gastos ───────────────────────────────────────────────────
with col_left:
    st.markdown('<p class="section-title">Distribuição por Faixa de Valor</p>', unsafe_allow_html=True)

    bucket_df = (
        dff.groupby("order_value_bucket")["order_id"]
        .nunique()
        .reset_index()
        .rename(columns={"order_id": "count"})
        .sort_values("count", ascending=True)
    )

    fig_bucket = px.bar(
        bucket_df,
        x="count",
        y="order_value_bucket",
        orientation="h",
        color="count",
        color_continuous_scale=["#1a1a2e", "#00e5a0"],
        labels={"order_value_bucket": "Faixa", "count": "Pedidos"},
    )

    fig_bucket.update_layout(
        plot_bgcolor="#0d0d0d",
        paper_bgcolor="#0d0d0d",
        font=dict(color="#f0f0f0", family="DM Sans"),
        coloraxis_showscale=False,
        xaxis=dict(showgrid=True, gridcolor="#1f1f1f"),
        yaxis=dict(showgrid=False),
        margin=dict(l=0, r=0, t=10, b=0),
        height=300,
    )
    fig_bucket.update_traces(marker_line_width=0)

    st.plotly_chart(fig_bucket, use_container_width=True)

# ── Right — vendas por mês com filtro de ano ──────────────────────────────────
with col_right:
    st.markdown('<p class="section-title">Vendas por Mês</p>', unsafe_allow_html=True)

    available_years = sorted(dff["year"].dropna().unique())
    selected_year = st.selectbox(
        "Ano",
        options=available_years,
        index=len(available_years) - 1
    )

    month_df = (
        dff[dff["year"] == selected_year]
        .groupby(["month", "month_name"])["value_items"]
        .sum()
        .reset_index()
        .sort_values("month")
    )

    fig_month = go.Figure()
    fig_month.add_trace(go.Scatter(
        x=month_df["month_name"],
        y=month_df["value_items"],
        mode="lines+markers",
        line=dict(color="#00e5a0", width=2.5),
        marker=dict(color="#00e5a0", size=7),
        fill="tozeroy",
        fillcolor="rgba(0, 229, 160, 0.08)",
    ))

    fig_month.update_layout(
        plot_bgcolor="#0d0d0d",
        paper_bgcolor="#0d0d0d",
        font=dict(color="#f0f0f0", family="DM Sans"),
        xaxis=dict(showgrid=False, tickfont=dict(size=11)),
        yaxis=dict(showgrid=True, gridcolor="#1f1f1f", tickprefix="R$ "),
        margin=dict(l=0, r=0, t=10, b=0),
        height=300,
    )

    st.plotly_chart(fig_month, use_container_width=True)