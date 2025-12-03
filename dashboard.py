import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title='Sales Dashboards', layout='wide')

datasets = {
    'DATA1': {
        'top5': [
            ('2024-12-17', 56980.860),
            ('2024-11-03', 46641.498),
            ('2025-03-23', 39120.974),
            ('2024-09-06', 33477.718),
            ('2025-01-25', 31650.924),
        ],
        'unique_users': 11237,
        'unique_author_sets': 325,
        'most_popular_author': 'Maynard Bartoletti Ret.',
        'best_buyers': [45800],
        'img': './results/DATA1.png',
    },
    'DATA2': {
        'top5': [
            ('2024-12-24', 42081.110),
            ('2024-08-29', 40556.078),
            ('2024-12-29', 39278.112),
            ('2025-01-30', 38954.788),
            ('2024-11-29', 35197.250),
        ],
        'unique_users': 9850,
        'unique_author_sets': 293,
        'most_popular_author': 'Hershel Treutel',
        'best_buyers': [53256],
        'img': './results/DATA2.png',
    },
    'DATA3': {
        'top5': [
            ('2025-02-03', 63983.640),
            ('2024-07-26', 38903.900),
            ('2024-11-03', 32831.300),
            ('2024-09-06', 31862.978),
            ('2024-09-20', 30629.450),
        ],
        'unique_users': 8933,
        'unique_author_sets': 268,
        'most_popular_author': 'Era Hodkiewicz',
        'best_buyers': [49414],
        'img': './results/DATA3.png',
    },
}


st.markdown(
    '''
    <style>
    .main > div.block-container{padding-top:0.35rem; padding-bottom:0.35rem;}
    .stMetricValue, .stMetricLabel {font-size:13px;}
    table.dataframe td, table.dataframe th {white-space: normal; word-break: break-word;}
    h1, h2, h3 {margin: 0.14rem 0;}
    </style>
    ''',
    unsafe_allow_html=True,
)

st.title('📊 Sales Dashboards — DATA1 / DATA2 / DATA3')

tabs = st.tabs(['DATA1', 'DATA2', 'DATA3'])

for tab_name, tab in zip(datasets.keys(), tabs):
    with tab:
        d = datasets[tab_name]

        c1, c2, c3, c4 = st.columns([1,1,1,1])
        c1.metric('Unique users', f'{d["unique_users"]:,}')
        c2.metric('Unique author sets', f'{d["unique_author_sets"]:,}')
        c3.markdown('**Most popular author**')
        c3.markdown(f'<div style="white-space:normal; font-size:13px">{d["most_popular_author"]}</div>', unsafe_allow_html=True)
        c4.markdown('**Best buyers**')
        c4.write(d['best_buyers'])

        st.markdown('---')

        left, right = st.columns([1.2, 0.8])
        with left:
            st.subheader('Top 5 Days by Revenue')
            df = pd.DataFrame(d['top5'], columns=['Date', 'Revenue'])
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df['Revenue'] = df['Revenue'].map(lambda x: f'${x:,.2f}')
            st.dataframe(df, use_container_width=True, height=200)

        st.markdown('---')
        st.subheader('Daily Revenue (chart image)')
        img_path = Path(d['img'])
        if img_path.exists():
            st.image(str(img_path))
        else:
            st.error(f'{d["img"]} not found in current directory')


st.markdown('---')
st.subheader('Directory listing (current directory)')
p = Path('.')
files = sorted([f.name for f in p.iterdir() if f.is_file()])

file_rows = []
for name in files:
    try:
        size = Path(name).stat().st_size
    except Exception:
        size = 0
    file_rows.append({'name': name, 'size_bytes': size})

st.table(pd.DataFrame(file_rows))
