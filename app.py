import streamlit as st
import pandas as pd
from itertools import combinations
import io
from datetime import datetime

# =========================
# PAGE CONFIG (MUST BE FIRST)
# =========================
st.set_page_config(
    page_title="HS Mapping Tool",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# PERFORMANCE: CACHING
# =========================
@st.cache_data(show_spinner=False)
def load_excel_file(file_content, filename):
    """Cache Excel file loading"""
    return pd.read_excel(io.BytesIO(file_content), dtype=str)

@st.cache_data(show_spinner=False)
def process_tree_data(df, market_name, digits=6):
    """Process and clean tree data - cached"""
    # Find HS column
    code_col = None
    for col in df.columns:
        col_lower = str(col).lower()
        if 'hs' in col_lower or 'hts' in col_lower or 'code' in col_lower:
            code_col = col
            break
    if code_col is None:
        code_col = df.columns[0]
    
    # Clean data
    result = pd.DataFrame()
    result["hs_code"] = df[code_col].astype(str).str.strip()
    result["hs_code"] = result["hs_code"].str.replace(r'\.0$', '', regex=True)
    result["hs_code"] = result["hs_code"].str.replace(r'[^0-9]', '', regex=True)
    result = result[result["hs_code"].str.len() >= 4]
    result["prefix"] = result["hs_code"].str[:digits]
    result["market"] = market_name
    
    return result[["market", "hs_code", "prefix"]], code_col, len(result)

@st.cache_data(show_spinner=False)
def build_prefix_lookup(_df, markets):
    """Build fast lookup dictionary - cached"""
    lookup = {}
    
    # Group by prefix first (faster)
    grouped = _df.groupby("prefix")
    
    for prefix, group in grouped:
        lookup[prefix] = {}
        for market in markets:
            codes = group[group.market == market]["hs_code"].unique().tolist()
            lookup[prefix][market] = codes
    
    return lookup

@st.cache_data(show_spinner=False)
def analyze_pairs_cached(_lookup_keys, _lookup_values, markets):
    """Analyze pairs - cached"""
    # Reconstruct lookup from keys/values (for caching)
    lookup = dict(zip(_lookup_keys, _lookup_values))
    
    results = {}
    stats = {}
    
    for A, B in combinations(markets, 2):
        rows = []
        pair_stats = {"one_to_one": 0, "one_to_many": 0, "many_to_one": 0, "many_to_many": 0, "no_match": 0}
        
        for prefix, market_codes in lookup.items():
            codes_A = market_codes.get(A, [])
            codes_B = market_codes.get(B, [])
            cA, cB = len(codes_A), len(codes_B)
            
            if cA == 0 or cB == 0:
                relation = "No Match"
                pair_stats["no_match"] += 1
            elif cA == 1 and cB == 1:
                relation = "One-to-One"
                pair_stats["one_to_one"] += 1
            elif cA == 1 and cB > 1:
                relation = "One-to-Many"
                pair_stats["one_to_many"] += 1
            elif cA > 1 and cB == 1:
                relation = "Many-to-One"
                pair_stats["many_to_one"] += 1
            else:
                relation = "Many-to-Many"
                pair_stats["many_to_many"] += 1
            
            row = {"HS Prefix": prefix, "Relation": relation, f"{A}_Count": cA, f"{B}_Count": cB}
            
            # Add codes (max 5 for speed)
            for i, code in enumerate(sorted(codes_A)[:5], 1):
                row[f"{A}_Code_{i}"] = code
            for i, code in enumerate(sorted(codes_B)[:5], 1):
                row[f"{B}_Code_{i}"] = code
            
            rows.append(row)
        
        results[(A, B)] = pd.DataFrame(rows)
        
        total = pair_stats["one_to_one"] + pair_stats["one_to_many"] + pair_stats["many_to_one"] + pair_stats["many_to_many"]
        pair_stats["total_shared"] = total
        pair_stats["percentage"] = (pair_stats["one_to_one"] / total * 100) if total > 0 else 0
        stats[(A, B)] = pair_stats
    
    return results, stats

@st.cache_data(show_spinner=False)
def get_strict_one_to_one_cached(_lookup_keys, _lookup_values, markets):
    """Get strict one-to-one - cached"""
    lookup = dict(zip(_lookup_keys, _lookup_values))
    
    rows = []
    for prefix, market_codes in lookup.items():
        is_strict = True
        codes_found = {}
        markets_with_codes = 0
        
        for market in markets:
            codes = market_codes.get(market, [])
            if len(codes) > 0:
                markets_with_codes += 1
                if len(codes) == 1:
                    codes_found[market] = codes[0]
                else:
                    is_strict = False
                    break
        
        if is_strict and markets_with_codes >= 2:
            row = {"HS Prefix": prefix}
            for market in markets:
                row[f"Code_{market}"] = codes_found.get(market, "")
            rows.append(row)
    
    return pd.DataFrame(rows)

def map_asins_fast(asin_df, lookup, source_market, target_markets):
    """Map ASINs - optimized"""
    results = []
    
    # Pre-fetch all prefixes
    prefixes = asin_df["prefix"].unique()
    
    # Build mini lookup for relevant prefixes only
    mini_lookup = {p: lookup.get(p, {}) for p in prefixes}
    
    for _, row in asin_df.iterrows():
        prefix = row["prefix"]
        market_codes = mini_lookup.get(prefix, {})
        
        result = {
            "ASIN": row["ASIN"],
            "Source_Code": row["hs_code"],
            "Prefix": prefix
        }
        
        source_codes = market_codes.get(source_market, [])
        sc = len(source_codes)
        
        for target in target_markets:
            target_codes = market_codes.get(target, [])
            tc = len(target_codes)
            
            if tc == 0:
                relation = "No Match"
            elif sc == 1 and tc == 1:
                relation = "One-to-One"
            elif sc == 1 and tc > 1:
                relation = "One-to-Many"
            elif sc > 1 and tc == 1:
                relation = "Many-to-One"
            else:
                relation = "Many-to-Many"
            
            result[f"{target}_Relation"] = relation
            for i, code in enumerate(sorted(target_codes)[:5], 1):
                result[f"{target}_Code_{i}"] = code
        
        results.append(result)
    
    return pd.DataFrame(results)

# =========================
# SESSION STATE
# =========================
if 'trees' not in st.session_state:
    st.session_state.trees = {}
if 'analysis_done' not in st.session_state:
    st.session_state.analysis_done = False

DIGITS = 6

# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.title("🌐 HS Mapping Tool")
    st.markdown("---")
    
    page = st.radio("📍 Navigation", [
        "🏠 Home",
        "📁 Load Trees",
        "📊 Analysis",
        "📦 ASIN Mapping",
        "📥 Export"
    ])
    
    st.markdown("---")
    st.subheader("📊 Loaded Trees")
    
    if st.session_state.trees:
        for market, data in st.session_state.trees.items():
            st.success(f"✅ {market}: {data['count']:,}")
    else:
        st.info("No trees loaded")
    
    # Clear cache button
    st.markdown("---")
    if st.button("🗑️ Clear Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")

# =========================
# PAGES
# =========================

if page == "🏠 Home":
    st.title("🌐 HS Code Mapping Tool")
    
    st.markdown("""
    ### Welcome! 👋
    
    This tool helps you:
    - 📁 **Load** marketplace HS code trees
    - 🔄 **Compare** codes across marketplaces  
    - ✅ **Find** One-to-One matches
    - 📦 **Map** ASINs to other marketplaces
    - 📥 **Export** results to Excel/CSV
    
    ---
    
    ### 🚀 Quick Start:
    1. **📁 Load Trees** → Upload your marketplace files
    2. **📊 Analysis** → Compare and analyze
    3. **📦 ASIN Mapping** → Map your products
    4. **📥 Export** → Download results
    
    ---
    
    ### ⚡ Performance Tips:
    - Data is cached for faster reloads
    - Analysis runs once and stores results
    - Use CSV export for large datasets
    """)
    
    if st.session_state.analysis_done:
        st.success("✅ Analysis is ready! Go to 📊 Analysis to view results.")

elif page == "📁 Load Trees":
    st.title("📁 Load Marketplace Trees")
    
    col1, col2 = st.columns(2)
    
    with col1:
        uploaded_file = st.file_uploader("📤 Upload Excel file", type=['xlsx', 'xls'])
    
    with col2:
        market_name = st.text_input("🏪 Market Name", placeholder="e.g., UAE, KSA, EGY, USA")
    
    if uploaded_file and market_name:
        if st.button("➕ Add Tree", type="primary"):
            with st.spinner("Loading..."):
                # Read file content once
                file_content = uploaded_file.read()
                
                # Load and process (cached)
                df = load_excel_file(file_content, uploaded_file.name)
                processed_df, col_used, count = process_tree_data(df, market_name.upper(), DIGITS)
                
                st.session_state.trees[market_name.upper()] = {
                    "df": processed_df,
                    "count": count,
                    "column": col_used
                }
                st.session_state.analysis_done = False
                
                st.success(f"✅ Loaded {count:,} codes for {market_name.upper()}")
                st.rerun()
    
    st.markdown("---")
    
    if st.session_state.trees:
        st.subheader("📋 Loaded Trees")
        
        cols = st.columns(len(st.session_state.trees))
        
        for i, (market, data) in enumerate(st.session_state.trees.items()):
            with cols[i]:
                st.metric(f"🏪 {market}", f"{data['count']:,} codes")
                
                with st.expander("View sample"):
                    st.dataframe(data['df'].head(5), use_container_width=True)
                
                if st.button(f"🗑️ Remove", key=f"rm_{market}"):
                    del st.session_state.trees[market]
                    st.session_state.analysis_done = False
                    st.rerun()

elif page == "📊 Analysis":
    st.title("📊 Analysis")
    
    if len(st.session_state.trees) < 2:
        st.warning("⚠️ Load at least 2 marketplace trees first!")
        st.info("Go to **📁 Load Trees** to upload files.")
    else:
        # Run Analysis Button
        if st.button("🚀 Run Analysis", type="primary") or st.session_state.analysis_done:
            
            if not st.session_state.analysis_done:
                with st.spinner("⚡ Analyzing... (this will be cached for next time)"):
                    # Combine trees
                    dfs = [data['df'] for data in st.session_state.trees.values()]
                    combined_df = pd.concat(dfs, ignore_index=True)
                    markets = list(st.session_state.trees.keys())
                    
                    # Build lookup
                    lookup = build_prefix_lookup(combined_df, tuple(markets))
                    
                    # Convert lookup for caching (dict is not hashable)
                    lookup_keys = tuple(lookup.keys())
                    lookup_values = tuple(tuple((k, tuple(v)) for k, v in val.items()) for val in lookup.values())
                    
                    # Rebuild lookup properly
                    lookup_rebuilt = {}
                    for key, val in zip(lookup_keys, lookup_values):
                        lookup_rebuilt[key] = {k: list(v) for k, v in val}
                    
                    # Analyze
                    results, stats = analyze_pairs_cached(lookup_keys, lookup_values, tuple(markets))
                    strict_df = get_strict_one_to_one_cached(lookup_keys, lookup_values, tuple(markets))
                    
                    # Store in session
                    st.session_state.combined_df = combined_df
                    st.session_state.lookup = lookup_rebuilt
                    st.session_state.results = results
                    st.session_state.stats = stats
                    st.session_state.strict_df = strict_df
                    st.session_state.markets = markets
                    st.session_state.analysis_done = True
            
            # Display Results
            stats = st.session_state.stats
            strict_df = st.session_state.strict_df
            markets = st.session_state.markets
            
            # Market Metrics
            st.subheader("📈 Market Overview")
            cols = st.columns(len(markets))
            for i, market in enumerate(markets):
                with cols[i]:
                    count = st.session_state.trees[market]['count']
                    st.metric(f"🏪 {market}", f"{count:,} codes")
            
            st.markdown("---")
            
            # Correlation Stats
            st.subheader("🔄 Correlation Statistics")
            
            stats_data = []
            for (A, B), s in stats.items():
                stats_data.append({
                    "Pair": f"{A} vs {B}",
                    "One-to-One": s["one_to_one"],
                    "One-to-Many": s["one_to_many"],
                    "Many-to-One": s["many_to_one"],
                    "Many-to-Many": s["many_to_many"],
                    "Total": s["total_shared"],
                    "Match %": f"{s['percentage']:.1f}%"
                })
            
            st.dataframe(pd.DataFrame(stats_data), use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # Strict One-to-One
            st.subheader(f"✅ Strict One-to-One: {len(strict_df):,} prefixes")
            
            # Search
            search = st.text_input("🔍 Search prefix", placeholder="Enter prefix...")
            
            display_df = strict_df
            if search:
                display_df = strict_df[strict_df["HS Prefix"].str.contains(search, na=False)]
                st.info(f"Found {len(display_df)} results")
            
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)
            
            st.markdown("---")
            
            # Pairwise Details
            st.subheader("📋 Detailed Pairwise Results")
            
            pair_options = [f"{A} vs {B}" for A, B in st.session_state.results.keys()]
            selected_pair = st.selectbox("Select pair", pair_options)
            
            if selected_pair:
                pair_key = tuple(selected_pair.split(" vs "))
                pair_df = st.session_state.results[pair_key]
                
                # Filter
                filter_relation = st.selectbox("Filter by relation", 
                    ["All", "One-to-One", "One-to-Many", "Many-to-One", "Many-to-Many", "No Match"])
                
                if filter_relation != "All":
                    pair_df = pair_df[pair_df["Relation"] == filter_relation]
                
                st.info(f"Showing {len(pair_df):,} rows")
                st.dataframe(pair_df, use_container_width=True, hide_index=True, height=400)

elif page == "📦 ASIN Mapping":
    st.title("📦 ASIN Mapping")
    
    if not st.session_state.analysis_done:
        st.warning("⚠️ Run Analysis first!")
        st.info("Go to **📊 Analysis** and click 'Run Analysis'")
    else:
        markets = st.session_state.markets
        
        col1, col2 = st.columns(2)
        
        with col1:
            asin_file = st.file_uploader("📤 Upload ASIN file", type=['xlsx', 'xls'])
        
        with col2:
            source_market = st.selectbox("🏪 Source Market", markets)
        
        if asin_file:
            # Load ASIN file
            asin_raw = pd.read_excel(asin_file, dtype=str)
            st.success(f"✅ Loaded {len(asin_raw):,} rows")
            
            col1, col2 = st.columns(2)
            with col1:
                asin_col = st.selectbox("ASIN Column", asin_raw.columns)
            with col2:
                hs_col = st.selectbox("HS Code Column", asin_raw.columns)
            
            if st.button("🚀 Map ASINs", type="primary"):
                with st.spinner("Mapping ASINs..."):
                    # Prepare data
                    asin_df = pd.DataFrame()
                    asin_df["ASIN"] = asin_raw[asin_col].astype(str).str.strip()
                    asin_df["hs_code"] = asin_raw[hs_col].astype(str).str.strip()
                    asin_df["hs_code"] = asin_df["hs_code"].str.replace(r'\.0$', '', regex=True)
                    asin_df["hs_code"] = asin_df["hs_code"].str.replace(r'[^0-9]', '', regex=True)
                    asin_df["prefix"] = asin_df["hs_code"].str[:DIGITS]
                    asin_df = asin_df[asin_df["hs_code"].str.len() >= 4]
                    
                    target_markets = [m for m in markets if m != source_market]
                    
                    # Map
                    result_df = map_asins_fast(asin_df, st.session_state.lookup, source_market, target_markets)
                    
                    st.session_state.asin_results = result_df
                    st.session_state.asin_source = source_market
                    
                    st.success(f"✅ Mapped {len(result_df):,} ASINs!")
        
        # Show results
        if 'asin_results' in st.session_state:
            st.markdown("---")
            st.subheader("📋 Mapping Results")
            
            result_df = st.session_state.asin_results
            
            # Summary
            st.write(f"**Total ASINs:** {len(result_df):,}")
            
            # Filter
            col1, col2 = st.columns(2)
            with col1:
                search_asin = st.text_input("🔍 Search ASIN", placeholder="Enter ASIN...")
            with col2:
                relation_cols = [c for c in result_df.columns if "_Relation" in c]
                if relation_cols:
                    filter_col = st.selectbox("Filter by", ["All"] + relation_cols)
            
            display_df = result_df
            if search_asin:
                display_df = display_df[display_df["ASIN"].str.contains(search_asin, na=False, case=False)]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

elif page == "📥 Export":
    st.title("📥 Export Results")
    
    if not st.session_state.analysis_done:
        st.warning("⚠️ Run Analysis first!")
    else:
        st.subheader("📊 Available Exports")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # One-to-One
            st.markdown("### ✅ One-to-One Matches")
            strict_df = st.session_state.strict_df
            st.write(f"{len(strict_df):,} prefixes")
            
            csv = strict_df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                "one_to_one.csv",
                "text/csv",
                type="primary"
            )
        
        with col2:
            # ASIN Results
            if 'asin_results' in st.session_state:
                st.markdown("### 📦 ASIN Mapping")
                st.write(f"{len(st.session_state.asin_results):,} ASINs")
                
                csv = st.session_state.asin_results.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    "asin_mapping.csv",
                    "text/csv",
                    type="primary"
                )
        
        st.markdown("---")
        
        # Pairwise Results
        st.subheader("📋 Pairwise Results")
        
        cols = st.columns(3)
        for i, ((A, B), result_df) in enumerate(st.session_state.results.items()):
            with cols[i % 3]:
                st.write(f"**{A} vs {B}**")
                st.write(f"{len(result_df):,} rows")
                
                csv = result_df.to_csv(index=False)
                st.download_button(
                    f"📥 Download",
                    csv,
                    f"{A}_{B}.csv",
                    "text/csv",
                    key=f"dl_{A}_{B}"
                )

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption("🌐 HS Mapping Tool v3.0 | ⚡ Optimized for speed")
