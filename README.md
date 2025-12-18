import streamlit as st
import pandas as pd
from itertools import combinations
import io
from datetime import datetime
import numpy as np

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="HS Mapping Tool",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================
# FIXED FUNCTIONS
# =========================

def detect_code_length(series):
    """Detect the most common/max code length in a series"""
    lengths = series.astype(str).str.replace(r'[^\d]', '', regex=True).str.len()
    lengths = lengths[lengths > 0]
    
    if len(lengths) == 0:
        return 12
    
    length_counts = lengths.value_counts()
    valid_lengths = length_counts[length_counts.index >= 6]
    
    if len(valid_lengths) > 0:
        return int(valid_lengths.index.max())
    
    return int(lengths.max()) if lengths.max() >= 6 else 12

def clean_hs_code(code, target_length=None):
    """Clean HS code - keeps only digits, preserves leading zeros"""
    if pd.isna(code):
        return ""
    
    code = str(code).strip()
    
    if 'e' in code.lower():
        try:
            num = int(float(code))
            code = str(num)
            if target_length and len(code) < target_length:
                code = code.zfill(target_length)
        except:
            pass
    
    if '.' in code:
        try:
            num = int(float(code))
            code = str(num)
            if target_length and len(code) < target_length:
                code = code.zfill(target_length)
        except:
            code = code.split('.')[0]
    
    code = ''.join(c for c in code if c.isdigit())
    
    if target_length and len(code) < target_length and len(code) > 0:
        code = code.zfill(target_length)
    
    return code

def clean_hs_code_vectorized(series, target_length=None):
    """Vectorized HS code cleaning - preserves leading zeros"""
    
    if target_length is None:
        target_length = detect_code_length(series)
    
    result = series.astype(str).str.strip()
    
    mask_sci = result.str.lower().str.contains('e', na=False)
    if mask_sci.any():
        def fix_scientific(x):
            try:
                num = int(float(x))
                code = str(num)
                if len(code) < target_length:
                    code = code.zfill(target_length)
                return code
            except:
                return x
        result.loc[mask_sci] = result.loc[mask_sci].apply(fix_scientific)
    
    mask_decimal = result.str.contains(r'\.', na=False) & ~mask_sci
    if mask_decimal.any():
        def fix_decimal(x):
            try:
                num = int(float(x))
                code = str(num)
                if len(code) < target_length:
                    code = code.zfill(target_length)
                return code
            except:
                return x.split('.')[0]
        result.loc[mask_decimal] = result.loc[mask_decimal].apply(fix_decimal)
    
    result = result.str.replace(r'[^\d]', '', regex=True)
    
    def pad_if_needed(x):
        if len(x) > 0 and len(x) < target_length:
            return x.zfill(target_length)
        return x
    
    result = result.apply(pad_if_needed)
    
    return result

def clean_asin(asin):
    """Clean ASIN - preserve as string"""
    if pd.isna(asin):
        return ""
    asin = str(asin).strip()
    if asin.endswith('.0'):
        asin = asin[:-2]
    return asin

def detect_asin_column(df):
    """Auto-detect ASIN column"""
    for col in df.columns:
        if 'asin' in str(col).lower():
            return col
    return df.columns[0]

def detect_hs_column(df, exclude_col=None):
    """Auto-detect HS code column"""
    for col in df.columns:
        if col == exclude_col:
            continue
        col_lower = str(col).lower()
        if 'hs' in col_lower or 'hts' in col_lower or 'tariff' in col_lower:
            return col
    
    for col in df.columns:
        if col == exclude_col:
            continue
        if 'code' in str(col).lower():
            return col
    
    for col in df.columns:
        if col != exclude_col:
            return col
    
    return df.columns[0]

@st.cache_data(show_spinner=False)
def load_excel_file(file_content, filename):
    """Load Excel as string"""
    try:
        df = pd.read_excel(io.BytesIO(file_content), dtype=str)
        return df, "success"
    except Exception as e:
        return None, str(e)

def process_tree_data(df, market_name, selected_column=None, expected_length=None):
    """Process tree data with proper leading zero handling"""
    
    diagnostics = {
        "raw_rows": len(df),
        "selected_column": None,
        "sample_raw": [],
        "sample_cleaned": [],
        "valid_codes": 0,
        "code_lengths": {},
        "detected_length": None
    }
    
    if selected_column and selected_column in df.columns:
        code_col = selected_column
    else:
        code_col = detect_hs_column(df)
    
    diagnostics["selected_column"] = code_col
    diagnostics["sample_raw"] = df[code_col].head(10).tolist()
    
    if expected_length is None:
        expected_length = detect_code_length(df[code_col])
    
    diagnostics["detected_length"] = expected_length
    
    result = pd.DataFrame()
    result["hs_code"] = clean_hs_code_vectorized(df[code_col], expected_length)
    
    diagnostics["sample_cleaned"] = result["hs_code"].head(10).tolist()
    
    result = result[result["hs_code"].str.len() > 0].copy()
    
    code_lengths = result["hs_code"].str.len().value_counts().to_dict()
    diagnostics["code_lengths"] = code_lengths
    
    for d in [4, 6, 7, 8, 9, 10, 12]:
        result[f"prefix_{d}"] = result["hs_code"].str[:d]
    
    result["market"] = market_name
    diagnostics["valid_codes"] = len(result)
    
    return result, diagnostics

def get_relation(count_a, count_b):
    if count_a == 0 or count_b == 0:
        return "No Match"
    elif count_a == 1 and count_b == 1:
        return "One-to-One"
    elif count_a == 1 and count_b > 1:
        return "One-to-Many"
    elif count_a > 1 and count_b == 1:
        return "Many-to-One"
    else:
        return "Many-to-Many"

def get_correlation_type(count):
    """Get correlation type based on count"""
    if count == 0:
        return "Deleted"
    elif count == 1:
        return "Direct"
    else:
        return "Indirect"

def compare_trees(old_df, new_df, digit_choice):
    """Compare old and new trees"""
    
    old_codes = clean_hs_code_vectorized(old_df["hs_code"])
    old_codes = old_codes[old_codes.str.len() >= digit_choice].unique()
    
    new_codes = clean_hs_code_vectorized(new_df["hs_code"])
    new_codes = new_codes[new_codes.str.len() >= digit_choice].unique()
    
    new_prefixes = pd.Series(new_codes).str[:digit_choice]
    new_lookup = pd.DataFrame({'code': new_codes, 'prefix': new_prefixes}).groupby('prefix')['code'].apply(list).to_dict()
    
    results = []
    stats = {
        "old_total": len(old_codes),
        "new_total": len(new_codes),
        "direct": 0,
        "indirect": 0,
        "deleted": 0,
        "new_codes": 0
    }
    
    old_prefixes_seen = set()
    for old_code in old_codes:
        prefix = old_code[:digit_choice]
        old_prefixes_seen.add(prefix)
        
        new_matches = new_lookup.get(prefix, [])
        correlation = len(new_matches)
        correlation_type = get_correlation_type(correlation)
        
        if correlation_type == "Direct":
            stats["direct"] += 1
        elif correlation_type == "Indirect":
            stats["indirect"] += 1
        else:
            stats["deleted"] += 1
        
        if correlation > 0:
            for new_code in new_matches:
                results.append({
                    "OldHSCode": old_code,
                    "OldPrefix": prefix,
                    "Correlation": correlation,
                    "CorrelationType": correlation_type,
                    "NewHSCode": new_code
                })
        else:
            results.append({
                "OldHSCode": old_code,
                "OldPrefix": prefix,
                "Correlation": 0,
                "CorrelationType": "Deleted",
                "NewHSCode": ""
            })
    
    for new_code in new_codes:
        prefix = new_code[:digit_choice]
        if prefix not in old_prefixes_seen:
            stats["new_codes"] += 1
            results.append({
                "OldHSCode": "",
                "OldPrefix": prefix,
                "Correlation": 0,
                "CorrelationType": "New",
                "NewHSCode": new_code
            })
    
    return pd.DataFrame(results), stats

def build_lookup_from_combined(combined_df, selected_markets, digit_choice):
    """Build lookup dictionary from combined dataframe"""
    prefix_col = f"prefix_{digit_choice}"
    
    grouped = combined_df.groupby([prefix_col, 'market'])['hs_code'].apply(
        lambda x: list(x.unique())
    ).reset_index()
    
    lookup = {}
    all_prefixes = grouped[prefix_col].unique()
    
    for prefix in all_prefixes:
        if prefix and len(prefix) > 0:
            lookup[prefix] = {m: [] for m in selected_markets}
            prefix_data = grouped[grouped[prefix_col] == prefix]
            for _, row in prefix_data.iterrows():
                lookup[prefix][row['market']] = row['hs_code']
    
    return lookup

def analyze_single_pair(lookup, market_a, market_b, digit_choice):
    """Analyze a single pair of markets"""
    rows = []
    stats = {
        "one_to_one": 0, 
        "one_to_many": 0, 
        "many_to_one": 0, 
        "many_to_many": 0, 
        "no_match": 0,
        "no_match_a_only": 0,
        "no_match_b_only": 0,
        "total_shared": 0,
        "total_prefixes": 0
    }
    
    for prefix, market_codes in lookup.items():
        codes_a = market_codes.get(market_a, [])
        codes_b = market_codes.get(market_b, [])
        ca, cb = len(codes_a), len(codes_b)
        
        if ca == 0 and cb == 0:
            continue
        
        stats["total_prefixes"] += 1
        
        if ca == 0:
            relation = f"No Match ({market_b}-Only)"
            source_mp = market_b
            stats["no_match"] += 1
            stats["no_match_b_only"] += 1
        elif cb == 0:
            relation = f"No Match ({market_a}-Only)"
            source_mp = market_a
            stats["no_match"] += 1
            stats["no_match_a_only"] += 1
        else:
            stats["total_shared"] += 1
            source_mp = "Both"
            relation = get_relation(ca, cb)
            key = relation.lower().replace("-", "_").replace(" ", "_")
            if key in stats:
                stats[key] += 1
        
        row = {
            f"Prefix_{digit_choice}d": prefix,
            "Source_MP": source_mp,
            "Relation": relation,
            f"{market_a}_Count": ca,
            f"{market_b}_Count": cb,
            f"In_{market_a}": "✓" if ca > 0 else "✗",
            f"In_{market_b}": "✓" if cb > 0 else "✗"
        }
        
        for i, code in enumerate(sorted(codes_a)[:5], 1):
            row[f"{market_a}_Code_{i}"] = code
        for i, code in enumerate(sorted(codes_b)[:5], 1):
            row[f"{market_b}_Code_{i}"] = code
        
        rows.append(row)
    
    return rows, stats

def analyze_all_pairs_optimized(lookup, selected_markets, digit_choice, progress_callback=None):
    """Analyze all pairs of markets"""
    
    all_pairwise_results = {}
    all_pairwise_stats = {}
    
    pairs = list(combinations(selected_markets, 2))
    total_pairs = len(pairs)
    
    for idx, (market_a, market_b) in enumerate(pairs):
        if progress_callback:
            progress_callback((idx + 1) / total_pairs, f"Analyzing {market_a} vs {market_b}...")
        
        rows, stats = analyze_single_pair(lookup, market_a, market_b, digit_choice)
        
        pair_key = f"{market_a}_vs_{market_b}"
        all_pairwise_results[pair_key] = pd.DataFrame(rows)
        all_pairwise_stats[pair_key] = stats
    
    return all_pairwise_results, all_pairwise_stats

def analyze_all_markets_comprehensive_optimized(lookup, selected_markets, digit_choice, progress_callback=None):
    """Create comprehensive view of all prefixes across all marketplaces"""
    
    rows = []
    prefixes = list(lookup.keys())
    total_prefixes = len(prefixes)
    
    batch_size = 1000
    
    for batch_start in range(0, total_prefixes, batch_size):
        batch_end = min(batch_start + batch_size, total_prefixes)
        batch_prefixes = prefixes[batch_start:batch_end]
        
        if progress_callback:
            progress_callback(batch_end / total_prefixes, f"Processing prefixes {batch_start}-{batch_end}...")
        
        for prefix in batch_prefixes:
            market_codes = lookup[prefix]
            counts = {market: len(market_codes.get(market, [])) for market in selected_markets}
            
            markets_with_code = [m for m, c in counts.items() if c > 0]
            markets_without_code = [m for m, c in counts.items() if c == 0]
            
            if len(markets_with_code) == len(selected_markets):
                source_mp = "All MPs"
                match_status = "Match (All MPs)"
            elif len(markets_with_code) == 1:
                source_mp = markets_with_code[0]
                match_status = f"{markets_with_code[0]}-Only"
            else:
                source_mp = "/".join(markets_with_code)
                match_status = f"Partial ({'/'.join(markets_with_code)})"
            
            row = {
                f"Prefix_{digit_choice}d": prefix,
                "Source_MP": source_mp,
                "Match_Status": match_status,
                "MPs_With_Code": len(markets_with_code),
                "MPs_Without_Code": len(markets_without_code)
            }
            
            for market in selected_markets:
                row[f"In_{market}"] = "✓" if counts[market] > 0 else "✗"
                row[f"{market}_Count"] = counts[market]
            
            for market in selected_markets:
                codes = market_codes.get(market, [])
                for i, code in enumerate(sorted(codes)[:3], 1):
                    row[f"{market}_Code_{i}"] = code
            
            rows.append(row)
    
    result_df = pd.DataFrame(rows)
    
    summary_stats = {
        "total_prefixes": len(rows),
        "all_mps_match": sum(1 for r in rows if r["MPs_With_Code"] == len(selected_markets)),
        "partial_match": sum(1 for r in rows if 1 < r["MPs_With_Code"] < len(selected_markets)),
    }
    
    for market in selected_markets:
        summary_stats[f"{market}_only"] = sum(1 for r in rows if r["Source_MP"] == market)
    
    return result_df, summary_stats

def create_executive_summary_excel(selected_markets, tree_counts, pairwise_stats, pairwise_results, digit_choice, comprehensive_df=None):
    """Create Excel file with Executive Summary"""
    
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        title_format = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': '#1F4E79'})
        header_format = workbook.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        cell_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
        section_format = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#D9E2F3'})
        green_format = workbook.add_format({'bg_color': '#92D050', 'border': 1, 'align': 'center'})
        yellow_format = workbook.add_format({'bg_color': '#FFEB9C', 'border': 1, 'align': 'center'})
        red_format = workbook.add_format({'bg_color': '#FFC7CE', 'border': 1, 'align': 'center'})
        percent_format = workbook.add_format({'border': 1, 'align': 'center', 'num_format': '0.00%'})
        
        ws = workbook.add_worksheet("Executive_Summary")
        
        ws.write(0, 0, "HS CODE MAPPING - EXECUTIVE SUMMARY", title_format)
        ws.write(1, 0, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        ws.write(2, 0, f"Matching Digits: {digit_choice}")
        
        row = 4
        ws.merge_range(row, 0, row, 3, "MARKETPLACE SUBHEADING COUNTS", section_format)
        
        row += 1
        for col, header in enumerate(["Marketplace", "Total Codes", "Unique Prefixes", "Code Length"]):
            ws.write(row, col, header, header_format)
        
        row += 1
        for market in selected_markets:
            ws.write(row, 0, market, cell_format)
            ws.write(row, 1, tree_counts[market]["total"], cell_format)
            ws.write(row, 2, tree_counts[market]["prefixes"], cell_format)
            ws.write(row, 3, tree_counts[market].get("code_length", "N/A"), cell_format)
            row += 1
        
        row += 2
        ws.merge_range(row, 0, row, 6, "MATCH ANALYSIS", section_format)
        
        row += 1
        headers = ["Marketplace Pair", "Shared Prefixes", "One-to-One", "Total Prefixes", "Overall Match %", "1:1 of Shared %", "Status"]
        for col, header in enumerate(headers):
            ws.write(row, col, header, header_format)
        
        row += 1
        for pair_key, stats in pairwise_stats.items():
            pair_name = pair_key.replace("_vs_", " vs ")
            
            total_prefixes = stats["total_prefixes"]
            total_shared = stats["total_shared"]
            one_to_one = stats["one_to_one"]
            
            overall_pct = (total_shared / total_prefixes * 100) if total_prefixes > 0 else 0
            shared_pct = (one_to_one / total_shared * 100) if total_shared > 0 else 0
            
            if overall_pct >= 70:
                status = "High Match"
                status_fmt = green_format
            elif overall_pct >= 40:
                status = "Medium Match"
                status_fmt = yellow_format
            else:
                status = "Low Match"
                status_fmt = red_format
            
            ws.write(row, 0, pair_name, cell_format)
            ws.write(row, 1, total_shared, cell_format)
            ws.write(row, 2, one_to_one, cell_format)
            ws.write(row, 3, total_prefixes, cell_format)
            ws.write(row, 4, overall_pct / 100, percent_format)
            ws.write(row, 5, shared_pct / 100, percent_format)
            ws.write(row, 6, status, status_fmt)
            row += 1
        
        row += 2
        ws.merge_range(row, 0, row, 7, "DETAILED RELATIONSHIP COUNTS", section_format)
        
        row += 1
        headers = ["Marketplace Pair", "One-to-One", "One-to-Many", "Many-to-One", "Many-to-Many", "MP-A Only", "MP-B Only", "Total Shared"]
        for col, header in enumerate(headers):
            ws.write(row, col, header, header_format)
        
        row += 1
        for pair_key, stats in pairwise_stats.items():
            pair_name = pair_key.replace("_vs_", " vs ")
            ws.write(row, 0, pair_name, cell_format)
            ws.write(row, 1, stats["one_to_one"], cell_format)
            ws.write(row, 2, stats["one_to_many"], cell_format)
            ws.write(row, 3, stats["many_to_one"], cell_format)
            ws.write(row, 4, stats["many_to_many"], cell_format)
            ws.write(row, 5, stats["no_match_a_only"], cell_format)
            ws.write(row, 6, stats["no_match_b_only"], cell_format)
            ws.write(row, 7, stats["total_shared"], cell_format)
            row += 1
        
        ws.set_column(0, 0, 22)
        ws.set_column(1, 7, 16)
        
        if comprehensive_df is not None:
            comprehensive_df.to_excel(writer, sheet_name="Comprehensive_All_MPs", index=False)
            ws_comp = writer.sheets["Comprehensive_All_MPs"]
            for col_num, col in enumerate(comprehensive_df.columns):
                ws_comp.write(0, col_num, col, header_format)
                ws_comp.set_column(col_num, col_num, 16)
        
        for pair_key, result_df in pairwise_results.items():
            sheet_name = pair_key[:31]
            result_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            ws_pair = writer.sheets[sheet_name]
            for col_num, col in enumerate(result_df.columns):
                ws_pair.write(0, col_num, col, header_format)
                ws_pair.set_column(col_num, col_num, 18)
    
    output.seek(0)
    return output

# =========================
# SESSION STATE
# =========================
if 'trees' not in st.session_state:
    st.session_state.trees = {}
if 'tree_diagnostics' not in st.session_state:
    st.session_state.tree_diagnostics = {}

# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.title("🌐 HS Mapping Tool")
    st.markdown("---")
    
    mode = st.radio("🎯 Select Mode", [
        "🏠 Home",
        "📁 Load Trees",
        "🔄 Tree-to-Tree Mapping",
        "📦 ASIN with HS Code Mapping",
        "🔃 Tree Comparison (Old vs New)",
        "🔍 Diagnose Issues"
    ])
    
    st.markdown("---")
    st.subheader("📊 Loaded Trees")
    
    if st.session_state.trees:
        for market, data in st.session_state.trees.items():
            code_len = data.get('code_length', '?')
            st.success(f"✅ {market}: {data['count']:,} ({code_len}d)")
    else:
        st.warning("No trees loaded")
    
    st.markdown("---")
    if st.button("🗑️ Clear All"):
        st.session_state.trees = {}
        st.session_state.tree_diagnostics = {}
        st.cache_data.clear()
        st.rerun()

# =========================
# HOME PAGE
# =========================
if mode == "🏠 Home":
    st.title("🌐 HS Code Mapping Tool")
    
    st.markdown("""
    ### Welcome! 👋
    
    **Features:**
    - ✅ **Tree-to-Tree Mapping** - Compare up to 5 markets at once
    - ✅ **ASIN Mapping** - Map to multiple trees with 4-12 digit options
    - ✅ **Tree Comparison** - Old vs New tree analysis
    - ✅ **Executive Summary** - Professional Excel export
    - ✅ **Leading Zero Fix** - Properly handles codes like 010121100001
    - ⚡ **Optimized Performance** - Fast processing with progress indicators
    
    ---
    
    ### ⚠️ Important: Leading Zeros
    
    HS codes starting with `0` (like `010121100001`) are now properly handled!
    The tool auto-detects code length and preserves leading zeros.
    
    ---
    
    ### 📊 Digit Matching Options:
    
    | Digits | Example | Use Case |
    |--------|---------|----------|
    | 4 | 0101 | Chapter level |
    | 6 | 010121 | Heading level |
    | 8 | 01012110 | Subheading |
    | 10 | 0101211000 | National level |
    | 12 | 010121100001 | Most specific |
    """)
    
    if st.session_state.trees:
        st.success(f"✅ {len(st.session_state.trees)} tree(s) loaded: {', '.join(st.session_state.trees.keys())}")

# =========================
# LOAD TREES PAGE
# =========================
elif mode == "📁 Load Trees":
    st.title("📁 Load Marketplace Trees")
    
    st.markdown("""
    ### Upload up to 5 marketplace trees
    
    **🔧 New:** The tool now auto-detects code length and preserves leading zeros!
    """)
    
    st.markdown("---")
    
    uploaded_file = st.file_uploader("📤 Upload Excel file", type=['xlsx', 'xls'])
    
    if uploaded_file:
        file_content = uploaded_file.read()
        df, status = load_excel_file(file_content, uploaded_file.name)
        
        if df is not None:
            st.success(f"✅ File loaded: {len(df):,} rows")
            
            st.markdown("---")
            
            detected_hs = detect_hs_column(df)
            detected_idx = list(df.columns).index(detected_hs) if detected_hs in df.columns else 0
            
            col1, col2 = st.columns(2)
            
            with col1:
                selected_column = st.selectbox("Select HS Code Column", df.columns, index=detected_idx)
            
            with col2:
                market_name = st.text_input("Market Name", placeholder="e.g., UAE, KSA, EGY, USA")
            
            if selected_column:
                detected_length = detect_code_length(df[selected_column])
                
                st.markdown("---")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"**Detected Code Length:** `{detected_length}` digits")
                
                with col2:
                    override_length = st.checkbox("Override length?")
                
                with col3:
                    if override_length:
                        expected_length = st.selectbox("Expected Length", [8, 10, 12], index=[8,10,12].index(detected_length) if detected_length in [8,10,12] else 2)
                    else:
                        expected_length = detected_length
                
                st.markdown("---")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Raw values:**")
                    for val in df[selected_column].head(5).tolist():
                        st.code(f"{val}")
                
                with col2:
                    st.markdown(f"**Cleaned (padded to {expected_length}d):**")
                    for val in df[selected_column].head(5).tolist():
                        cleaned = clean_hs_code(val, expected_length)
                        st.code(f"{cleaned} (len={len(cleaned)})")
                
                if market_name:
                    if len(st.session_state.trees) >= 5:
                        st.warning("⚠️ Maximum 5 trees allowed. Remove one to add another.")
                    else:
                        if st.button("➕ Add Tree", type="primary"):
                            with st.spinner("Processing tree data..."):
                                processed_df, diagnostics = process_tree_data(
                                    df, market_name.upper(), selected_column, expected_length
                                )
                                
                                st.session_state.trees[market_name.upper()] = {
                                    "df": processed_df,
                                    "count": len(processed_df),
                                    "code_length": expected_length
                                }
                                st.session_state.tree_diagnostics[market_name.upper()] = diagnostics
                                st.success(f"✅ Added {market_name.upper()} with {len(processed_df):,} codes ({expected_length}-digit)")
                                st.rerun()
    
    st.markdown("---")
    
    if st.session_state.trees:
        st.subheader(f"📋 Loaded Trees ({len(st.session_state.trees)}/5)")
        
        cols = st.columns(min(len(st.session_state.trees), 5))
        
        for i, (market, data) in enumerate(st.session_state.trees.items()):
            with cols[i]:
                code_len = data.get('code_length', '?')
                st.metric(f"🏪 {market}", f"{data['count']:,}", f"{code_len}-digit codes")
                if st.button(f"🗑️", key=f"rm_{market}"):
                    del st.session_state.trees[market]
                    st.rerun()

# =========================
# TREE-TO-TREE MAPPING
# =========================
elif mode == "🔄 Tree-to-Tree Mapping":
    st.title("🔄 Tree-to-Tree Mapping")
    
    if len(st.session_state.trees) < 2:
        st.warning("⚠️ Need at least 2 trees!")
        st.info("Go to **📁 Load Trees** to upload files.")
    else:
        markets = list(st.session_state.trees.keys())
        
        st.markdown("""
        ### Select Markets to Compare
        
        **🔧 Fixed Issues:**
        - ✅ Leading zeros now preserved (010121100001)
        - ✅ Match percentage now includes "Only" codes in calculation
        """)
        
        st.markdown("---")
        
        st.markdown("#### 📏 Loaded Tree Code Lengths")
        cols = st.columns(len(markets))
        for i, market in enumerate(markets):
            with cols[i]:
                code_len = st.session_state.trees[market].get('code_length', '?')
                st.info(f"**{market}**: {code_len}-digit")
        
        st.markdown("---")
        
        st.subheader("🏪 Select Markets")
        
        selected_markets = st.multiselect(
            "Select 2-5 markets to compare",
            markets,
            default=markets[:min(len(markets), 5)],
            max_selections=5,
            help="Select at least 2 and up to 5 markets"
        )
        
        if len(selected_markets) < 2:
            st.warning("⚠️ Select at least 2 markets")
            st.stop()
        
        st.success(f"✅ Selected {len(selected_markets)} markets: {', '.join(selected_markets)}")
        st.info(f"📊 This will generate {len(list(combinations(selected_markets, 2)))} pairwise comparisons")
        
        st.markdown("---")
        
        st.subheader("🔢 Matching Digits")
        
        digit_choice = st.select_slider(
            "Select digit level for prefix matching",
            options=[4, 6, 7, 8, 9, 10, 12],
            value=6,
            help="Lower = broader matching, Higher = more specific"
        )
        
        st.markdown(f"""
        **{digit_choice}-digit matching example:**
        - Full code: `010121100001`
        - Prefix used: `{('010121100001')[:digit_choice]}`
        """)
        
        st.markdown("---")
        
        if st.button("🚀 Run Multi-Market Comparison", type="primary"):
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            time_start = datetime.now()
            
            def update_progress(pct, msg):
                progress_bar.progress(pct)
                status_text.text(f"⏳ {msg}")
            
            update_progress(0.05, "Combining marketplace trees...")
            dfs = [st.session_state.trees[m]['df'] for m in selected_markets]
            combined = pd.concat(dfs, ignore_index=True)
            
            update_progress(0.15, f"Building lookup index for {len(combined):,} codes...")
            lookup = build_lookup_from_combined(combined, selected_markets, digit_choice)
            update_progress(0.30, f"Built lookup with {len(lookup):,} prefixes")
            
            def pair_progress(pct, msg):
                overall_pct = 0.30 + (pct * 0.40)
                update_progress(overall_pct, msg)
            
            pairwise_results, pairwise_stats = analyze_all_pairs_optimized(
                lookup, selected_markets, digit_choice, pair_progress
            )
            
            update_progress(0.70, "Building comprehensive view...")
            
            def comp_progress(pct, msg):
                overall_pct = 0.70 + (pct * 0.20)
                update_progress(overall_pct, msg)
            
            comprehensive_df, comprehensive_stats = analyze_all_markets_comprehensive_optimized(
                lookup, selected_markets, digit_choice, comp_progress
            )
            
            update_progress(0.90, "Calculating statistics...")
            
            tree_counts = {}
            for market in selected_markets:
                market_df = st.session_state.trees[market]['df']
                tree_counts[market] = {
                    "total": len(market_df),
                    "prefixes": market_df[f"prefix_{digit_choice}"].nunique(),
                    "code_length": st.session_state.trees[market].get('code_length', 'N/A')
                }
            
            st.session_state.multi_tree_results = pairwise_results
            st.session_state.multi_tree_stats = pairwise_stats
            st.session_state.multi_tree_counts = tree_counts
            st.session_state.multi_tree_markets = selected_markets
            st.session_state.multi_tree_digits = digit_choice
            st.session_state.comprehensive_df = comprehensive_df
            st.session_state.comprehensive_stats = comprehensive_stats
            
            update_progress(1.0, "Complete!")
            
            time_end = datetime.now()
            elapsed = (time_end - time_start).total_seconds()
            
            progress_bar.empty()
            status_text.empty()
            
            st.success(f"✅ Analysis complete in {elapsed:.1f} seconds!")
        
        if 'multi_tree_results' in st.session_state:
            pairwise_results = st.session_state.multi_tree_results
            pairwise_stats = st.session_state.multi_tree_stats
            tree_counts = st.session_state.multi_tree_counts
            selected_markets = st.session_state.multi_tree_markets
            digit_choice = st.session_state.multi_tree_digits
            comprehensive_df = st.session_state.comprehensive_df
            comprehensive_stats = st.session_state.comprehensive_stats
            
            st.markdown("---")
            st.subheader("📊 Executive Summary")
            
            st.markdown("#### 📁 Marketplace Subheading Counts")
            
            cols = st.columns(len(selected_markets))
            for i, market in enumerate(selected_markets):
                with cols[i]:
                    st.metric(
                        f"🏪 {market}",
                        f"{tree_counts[market]['total']:,}",
                        f"{tree_counts[market]['prefixes']:,} prefixes"
                    )
            
            st.markdown("---")
            
            st.markdown("#### 🌐 Overall Prefix Distribution")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Unique Prefixes", f"{comprehensive_stats['total_prefixes']:,}")
            with col2:
                st.metric("Match (All MPs)", f"{comprehensive_stats['all_mps_match']:,}")
            with col3:
                st.metric("Partial Match", f"{comprehensive_stats['partial_match']:,}")
            
            st.markdown("##### 📍 MP-Specific Codes (No Match in Other MPs)")
            cols = st.columns(len(selected_markets))
            for i, market in enumerate(selected_markets):
                with cols[i]:
                    only_count = comprehensive_stats.get(f"{market}_only", 0)
                    st.metric(f"{market}-Only", f"{only_count:,}")
            
            st.markdown("---")
            
            st.markdown("#### 🔄 Correlation Analysis")
            
            summary_data = []
            for pair_key, stats in pairwise_stats.items():
                pair_name = pair_key.replace("_vs_", " vs ")
                
                total_prefixes = stats["total_prefixes"]
                total_shared = stats["total_shared"]
                one_to_one = stats["one_to_one"]
                
                overall_pct = (total_shared / total_prefixes * 100) if total_prefixes > 0 else 0
                shared_pct = (one_to_one / total_shared * 100) if total_shared > 0 else 0
                
                if overall_pct >= 70:
                    status = "✅ High Match"
                elif overall_pct >= 40:
                    status = "⚠️ Medium Match"
                else:
                    status = "❌ Low Match"
                
                market_a, market_b = pair_key.split("_vs_")
                
                summary_data.append({
                    "Marketplace Pair": pair_name,
                    "Shared Prefixes": total_shared,
                    "One-to-One": one_to_one,
                    "One-to-Many": stats["one_to_many"],
                    "Many-to-One": stats["many_to_one"],
                    "Many-to-Many": stats["many_to_many"],
                    f"{market_a}-Only": stats["no_match_a_only"],
                    f"{market_b}-Only": stats["no_match_b_only"],
                    "Total Prefixes": total_prefixes,
                    "Overall Match %": f"{overall_pct:.2f}%",
                    "1:1 of Shared %": f"{shared_pct:.2f}%",
                    "Status": status
                })
            
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            
            st.markdown("""
            **📝 Percentage Explanation:**
            - **Overall Match %** = Shared Prefixes / Total Prefixes
            - **1:1 of Shared %** = One-to-One / Shared Prefixes
            """)
            
            st.markdown("---")
            
            st.subheader("📋 Detailed Results")
            
            view_tab = st.radio(
                "Select View",
                ["🌐 Comprehensive (All MPs)", "🔄 Pairwise Comparison"],
                horizontal=True
            )
            
            if view_tab == "🌐 Comprehensive (All MPs)":
                col1, col2, col3 = st.columns(3)
                with col1:
                    filter_status = st.selectbox(
                        "Filter by Match Status",
                        ["All", "Match (All MPs)", "Partial Match", "Single MP Only"]
                    )
                with col2:
                    filter_source = st.selectbox(
                        "Filter by Source MP",
                        ["All"] + selected_markets + ["All MPs"]
                    )
                with col3:
                    search_prefix = st.text_input("🔍 Search Prefix")
                
                display_comprehensive = comprehensive_df.copy()
                
                if filter_status == "Match (All MPs)":
                    display_comprehensive = display_comprehensive[display_comprehensive["Match_Status"] == "Match (All MPs)"]
                elif filter_status == "Partial Match":
                    display_comprehensive = display_comprehensive[display_comprehensive["Match_Status"].str.startswith("Partial")]
                elif filter_status == "Single MP Only":
                    display_comprehensive = display_comprehensive[display_comprehensive["MPs_With_Code"] == 1]
                
                if filter_source != "All":
                    if filter_source == "All MPs":
                        display_comprehensive = display_comprehensive[display_comprehensive["Source_MP"] == "All MPs"]
                    else:
                        display_comprehensive = display_comprehensive[display_comprehensive["Source_MP"].str.contains(filter_source, na=False)]
                
                if search_prefix:
                    display_comprehensive = display_comprehensive[
                        display_comprehensive[f"Prefix_{digit_choice}d"].str.contains(search_prefix, na=False)
                    ]
                
                st.info(f"Showing {len(display_comprehensive):,} of {len(comprehensive_df):,} prefixes")
                st.dataframe(display_comprehensive, use_container_width=True, hide_index=True, height=400)
                
            else:
                pair_options = list(pairwise_results.keys())
                selected_pair = st.selectbox(
                    "Select pair to view details",
                    pair_options,
                    format_func=lambda x: x.replace("_vs_", " vs ")
                )
                
                if selected_pair:
                    pair_df = pairwise_results[selected_pair]
                    pair_stats = pairwise_stats[selected_pair]
                    
                    market_a, market_b = selected_pair.split("_vs_")
                    
                    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
                    col1.metric("One-to-One", f"{pair_stats['one_to_one']:,}")
                    col2.metric("One-to-Many", f"{pair_stats['one_to_many']:,}")
                    col3.metric("Many-to-One", f"{pair_stats['many_to_one']:,}")
                    col4.metric("Many-to-Many", f"{pair_stats['many_to_many']:,}")
                    col5.metric("Shared", f"{pair_stats['total_shared']:,}")
                    col6.metric(f"{market_a}-Only", f"{pair_stats['no_match_a_only']:,}")
                    col7.metric(f"{market_b}-Only", f"{pair_stats['no_match_b_only']:,}")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        filter_options = [
                            "All", "One-to-One", "One-to-Many", "Many-to-One", "Many-to-Many",
                            f"No Match ({market_a}-Only)", f"No Match ({market_b}-Only)", "All No Match"
                        ]
                        filter_rel = st.selectbox("Filter by Relation", filter_options)
                    with col2:
                        filter_source = st.selectbox("Filter by Source MP", ["All", market_a, market_b, "Both"], key="pairwise_source_filter")
                    with col3:
                        search = st.text_input("🔍 Search Prefix", key="pairwise_search")
                    
                    display_df = pair_df.copy()
                    
                    if filter_rel == "All No Match":
                        display_df = display_df[display_df["Relation"].str.startswith("No Match")]
                    elif filter_rel != "All":
                        display_df = display_df[display_df["Relation"] == filter_rel]
                    
                    if filter_source != "All":
                        display_df = display_df[display_df["Source_MP"] == filter_source]
                    
                    if search:
                        prefix_col = f"Prefix_{digit_choice}d"
                        display_df = display_df[display_df[prefix_col].str.contains(search, na=False)]
                    
                    st.info(f"Showing {len(display_df):,} of {len(pair_df):,} rows")
                    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)
            
            st.markdown("---")
            
            st.subheader("📥 Download Results")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**📊 Full Excel Report**")
                
                with st.spinner("Generating Excel..."):
                    excel_file = create_executive_summary_excel(
                        selected_markets, tree_counts, pairwise_stats, pairwise_results, digit_choice, comprehensive_df
                    )
                
                st.download_button(
                    "📥 Download Full Excel Report",
                    excel_file,
                    f"HS_Mapping_Report_{digit_choice}d_{len(selected_markets)}markets.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
            
            with col2:
                st.markdown("**🌐 Comprehensive View CSV**")
                comprehensive_csv = comprehensive_df.to_csv(index=False)
                st.download_button(
                    "📥 Download Comprehensive View",
                    comprehensive_csv,
                    f"Comprehensive_All_MPs_{digit_choice}d.csv",
                    "text/csv"
                )
            
            with col3:
                st.markdown("**📋 Individual Pair CSVs**")
                for pair_key, pair_df in pairwise_results.items():
                    csv = pair_df.to_csv(index=False)
                    st.download_button(
                        f"📥 {pair_key.replace('_vs_', ' vs ')}",
                        csv,
                        f"{pair_key}_{digit_choice}d.csv",
                        "text/csv",
                        key=f"dl_{pair_key}"
                    )

# =========================
# TREE COMPARISON (OLD vs NEW)
# =========================
elif mode == "🔃 Tree Comparison (Old vs New)":
    st.title("🔃 Tree Comparison (Old vs New)")
    
    st.markdown("""
    ### Compare old and new HS code trees
    Find: Direct (1:1), Indirect (1:Many), Deleted, New codes
    """)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📂 Old Tree")
        old_file = st.file_uploader("Upload OLD tree", type=['xlsx', 'xls'], key="old_tree")
    
    with col2:
        st.subheader("📂 New Tree")
        new_file = st.file_uploader("Upload NEW tree", type=['xlsx', 'xls'], key="new_tree")
    
    if old_file and new_file:
        old_content = old_file.read()
        new_content = new_file.read()
        
        old_df, _ = load_excel_file(old_content, old_file.name)
        new_df, _ = load_excel_file(new_content, new_file.name)
        
        if old_df is not None and new_df is not None:
            st.success(f"✅ Old: {len(old_df):,} rows | New: {len(new_df):,} rows")
            
            st.markdown("---")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                old_hs_col = st.selectbox("Old Tree HS Column", old_df.columns)
            with col2:
                new_hs_col = st.selectbox("New Tree HS Column", new_df.columns)
            with col3:
                digit_choice = st.selectbox("Mapping Digits", [4, 6, 7, 8, 9, 10, 12], index=4)
            
            if st.button("🚀 Compare Trees", type="primary"):
                with st.spinner("Comparing..."):
                    old_processed = pd.DataFrame()
                    old_processed["hs_code"] = old_df[old_hs_col]
                    
                    new_processed = pd.DataFrame()
                    new_processed["hs_code"] = new_df[new_hs_col]
                    
                    result_df, stats = compare_trees(old_processed, new_processed, digit_choice)
                    
                    st.session_state.comparison_result = result_df
                    st.session_state.comparison_stats = stats
                    st.session_state.comparison_digits = digit_choice
    
    if 'comparison_result' in st.session_state:
        st.markdown("---")
        
        stats = st.session_state.comparison_stats
        result_df = st.session_state.comparison_result
        
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Old Codes", f"{stats['old_total']:,}")
        col2.metric("New Codes", f"{stats['new_total']:,}")
        col3.metric("Direct", f"{stats['direct']:,}")
        col4.metric("Indirect", f"{stats['indirect']:,}")
        col5.metric("Deleted", f"{stats['deleted']:,}")
        
        st.dataframe(result_df, use_container_width=True, hide_index=True, height=400)
        
        csv = result_df.to_csv(index=False)
        st.download_button("📥 Download", csv, "tree_comparison.csv", "text/csv", type="primary")

# =========================
# ASIN WITH HS CODE MAPPING
# =========================
elif mode == "📦 ASIN with HS Code Mapping":
    st.title("📦 ASIN with HS Code Mapping")
    
    if len(st.session_state.trees) < 1:
        st.warning("⚠️ Load at least 1 tree first!")
    else:
        markets = list(st.session_state.trees.keys())
        
        st.markdown("---")
        
        asin_file = st.file_uploader("📤 Upload ASIN file", type=['xlsx', 'xls'])
        
        if asin_file:
            file_content = asin_file.read()
            asin_raw, _ = load_excel_file(file_content, asin_file.name)
            
            if asin_raw is not None:
                st.success(f"✅ Loaded {len(asin_raw):,} rows")
                
                st.markdown("---")
                
                detected_asin = detect_asin_column(asin_raw)
                detected_hs = detect_hs_column(asin_raw, exclude_col=detected_asin)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    asin_col = st.selectbox("ASIN Column", asin_raw.columns,
                        index=list(asin_raw.columns).index(detected_asin) if detected_asin in asin_raw.columns else 0)
                
                with col2:
                    hs_col = st.selectbox("HS Code Column", asin_raw.columns,
                        index=list(asin_raw.columns).index(detected_hs) if detected_hs in asin_raw.columns else 1)
                
                if asin_col == hs_col:
                    st.error("❌ Columns must be different!")
                    st.stop()
                
                st.markdown("---")
                
                detected_length = detect_code_length(asin_raw[hs_col])
                st.info(f"📏 Detected code length in ASIN file: **{detected_length}** digits")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    digit_choice = st.selectbox("Matching Digits", [4, 6, 7, 8, 9, 10, 12], index=2)
                    source_market = st.text_input("Source Market", placeholder="e.g., AE")
                    if source_market:
                        source_market = source_market.upper()
                
                with col2:
                    target_markets = st.multiselect("Target Markets", markets, default=markets)
                
                if source_market and target_markets:
                    if st.button("🚀 Run Mapping", type="primary"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        status_text.text("⏳ Preparing ASIN data...")
                        progress_bar.progress(0.1)
                        
                        asin_df = pd.DataFrame()
                        asin_df["ASIN"] = asin_raw[asin_col].astype(str).str.strip()
                        asin_df["hs_code"] = clean_hs_code_vectorized(asin_raw[hs_col], detected_length)
                        asin_df["prefix"] = asin_df["hs_code"].str[:digit_choice]
                        asin_df = asin_df[asin_df["prefix"].str.len() == digit_choice]
                        asin_df = asin_df[asin_df["ASIN"].str.len() > 0]
                        
                        progress_bar.progress(0.3)
                        status_text.text("⏳ Building lookup...")
                        
                        dfs = [st.session_state.trees[m]['df'] for m in target_markets]
                        combined = pd.concat(dfs, ignore_index=True)
                        
                        lookup = build_lookup_from_combined(combined, target_markets, digit_choice)
                        
                        progress_bar.progress(0.5)
                        status_text.text(f"⏳ Mapping {len(asin_df):,} ASINs...")
                        
                        results = []
                        total_asins = len(asin_df)
                        
                        for idx, (_, row) in enumerate(asin_df.iterrows()):
                            if idx % 1000 == 0:
                                pct = 0.5 + (idx / total_asins * 0.4)
                                progress_bar.progress(pct)
                                status_text.text(f"⏳ Mapping ASINs... {idx:,}/{total_asins:,}")
                            
                            result = {
                                "ASIN": row["ASIN"],
                                "Source_HS_Code": row["hs_code"],
                                f"Prefix_{digit_choice}d": row["prefix"],
                                "Source_Market": source_market
                            }
                            
                            market_codes = lookup.get(row["prefix"], {})
                            
                            for target in target_markets:
                                target_codes = market_codes.get(target, [])
                                tc = len(target_codes)
                                
                                result[f"{target}_Relation"] = "No Match" if tc == 0 else ("One-to-One" if tc == 1 else "One-to-Many")
                                result[f"{target}_Count"] = tc
                                for i, code in enumerate(sorted(target_codes)[:5], 1):
                                    result[f"{target}_Code_{i}"] = code
                            
                            results.append(result)
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ Complete!")
                        
                        st.session_state.asin_results = pd.DataFrame(results)
                        st.session_state.asin_targets = target_markets
                        
                        progress_bar.empty()
                        status_text.empty()
                        
                        st.success(f"✅ Mapped {len(results):,} ASINs!")
        
        if 'asin_results' in st.session_state:
            result_df = st.session_state.asin_results
            targets = st.session_state.asin_targets
            
            st.markdown("---")
            
            cols = st.columns(len(targets))
            for i, target in enumerate(targets):
                with cols[i]:
                    counts = result_df[f"{target}_Relation"].value_counts()
                    st.metric(f"{target}", f"{counts.get('One-to-One', 0):,} 1:1")
            
            st.dataframe(result_df, use_container_width=True, hide_index=True, height=400)
            
            csv = result_df.to_csv(index=False)
            st.download_button("📥 Download", csv, "asin_mapping.csv", "text/csv", type="primary")

# =========================
# DIAGNOSE ISSUES
# =========================
elif mode == "🔍 Diagnose Issues":
    st.title("🔍 Diagnose Issues")
    
    st.markdown("### Debug HS Code Matching")
    
    if len(st.session_state.trees) < 1:
        st.warning("⚠️ Load at least 1 tree!")
    else:
        markets = list(st.session_state.trees.keys())
        
        st.markdown("---")
        
        st.subheader("📊 Loaded Trees Info")
        for market in markets:
            data = st.session_state.trees[market]
            diag = st.session_state.tree_diagnostics.get(market, {})
            code_len = data.get('code_length', 'Unknown')
            
            with st.expander(f"🏪 {market} - {data['count']:,} codes ({code_len}-digit)"):
                st.write(f"**Detected code length:** {diag.get('detected_length', 'N/A')}")
                st.write(f"**Code length distribution:**")
                st.json(diag.get('code_lengths', {}))
                st.write(f"**Sample raw values:**")
                st.code("\n".join(str(x) for x in diag.get('sample_raw', [])[:5]))
                st.write(f"**Sample cleaned values:**")
                st.code("\n".join(str(x) for x in diag.get('sample_cleaned', [])[:5]))
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            test_code = st.text_input("Enter HS Code to Test", placeholder="e.g., 010121100001")
        with col2:
            digit_choice = st.selectbox("Matching Digits", [4, 6, 7, 8, 9, 10, 12], index=2)
        
        if test_code:
            st.markdown("---")
            
            st.subheader("🔬 Code Processing Analysis")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write("**Original Input:**")
                st.code(test_code)
            
            with col2:
                cleaned = clean_hs_code(test_code, 12)
                st.write("**Cleaned (12-digit padding):**")
                st.code(f"{cleaned} (len={len(cleaned)})")
            
            with col3:
                prefix = cleaned[:digit_choice] if len(cleaned) >= digit_choice else cleaned
                st.write(f"**Prefix ({digit_choice}d):**")
                st.code(prefix)
            
            st.markdown("---")
            
            st.subheader("🔍 Search Results by Marketplace")
            
            for market in markets:
                market_df = st.session_state.trees[market]['df']
                code_len = st.session_state.trees[market].get('code_length', 12)
                
                cleaned_for_market = clean_hs_code(test_code, code_len)
                prefix_for_market = cleaned_for_market[:digit_choice]
                
                exact_match = market_df[market_df['hs_code'] == cleaned_for_market]
                
                prefix_col = f"prefix_{digit_choice}"
                prefix_match = market_df[market_df[prefix_col] == prefix_for_market]
                
                with st.expander(f"🏪 {market} ({code_len}-digit codes)"):
                    st.write(f"**Cleaned code for this MP:** `{cleaned_for_market}`")
                    st.write(f"**Prefix used:** `{prefix_for_market}`")
                    
                    if len(exact_match) > 0:
                        st.success(f"✅ Exact match found!")
                        st.dataframe(exact_match[['hs_code', 'market']].head(5))
                    else:
                        st.warning(f"❌ No exact match for `{cleaned_for_market}`")
                    
                    if len(prefix_match) > 0:
                        st.success(f"✅ {len(prefix_match)} codes with prefix `{prefix_for_market}`")
                        st.dataframe(prefix_match[['hs_code', 'market']].head(10))
                    else:
                        st.error(f"❌ No codes with prefix `{prefix_for_market}`")
                    
                    similar = market_df[market_df['hs_code'].str[:4] == cleaned_for_market[:4]]
                    if len(similar) > 0:
                        st.write(f"**Similar codes (same first 4 digits):**")
                        st.code("\n".join(similar['hs_code'].head(10).tolist()))

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption("🌐 HS Mapping Tool v14.0 | Fixed Leading Zeros + Corrected Match Percentages")
