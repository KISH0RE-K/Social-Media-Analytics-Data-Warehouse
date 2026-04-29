import dash
from dash import dcc, html, callback, Output, Input, State
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import pandas as pd
from datetime import datetime
import os


app = dash.Dash(
    __name__,
    title="Network Intelligence",
    update_title=None,
    external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css",
    ]
)


app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap" rel="stylesheet">
        <style>
            * { box-sizing: border-box; }
            body { 
                font-family: 'Poppins', sans-serif;
                background: linear-gradient(135deg, #0a0a0f 0%, #1a1a2e 50%, #0f0f1a 100%);
                min-height: 100vh;
                color: #e0e0e0;
            }
            .card-custom {
                background: rgba(20, 20, 35, 0.8);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 16px;
                transition: all 0.3s ease;
            }
            .card-custom:hover {
                transform: translateY(-4px);
                border-color: rgba(0, 242, 255, 0.3);
                box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
            }
            .kpi-card {
                background: linear-gradient(145deg, rgba(30, 30, 50, 0.9), rgba(20, 20, 35, 0.95));
                border-left: 4px solid;
            }
            .kpi-value {
                font-family: 'Poppins', sans-serif;
                font-size: 2.5rem;
                font-weight: 600;
                background: linear-gradient(90deg, #00f2ff, #7000ff);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .chart-container {
                background: rgba(15, 15, 25, 0.6);
                border-radius: 12px;
                padding: 20px;
            }
            .btn-refresh {
                background: linear-gradient(135deg, #00f2ff 0%, #0080ff 100%);
                border: none;
                padding: 12px 28px;
                border-radius: 30px;
                font-weight: 600;
                transition: all 0.3s ease;
            }
            .btn-refresh:hover {
                transform: scale(1.05);
                box-shadow: 0 10px 30px rgba(0, 242, 255, 0.3);
            }
            .header-title {
                font-family: 'Poppins', sans-serif;
                letter-spacing: 4px;
                background: linear-gradient(90deg, #00f2ff, #7000ff, #ff007a);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .loading-spinner {
                border: 3px solid rgba(0, 242, 255, 0.1);
                border-top: 3px solid #00f2ff;
                border-radius: 50%;
                width: 30px;
                height: 30px;
                animation: spin 1s linear infinite;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            .pulse-dot {
                width: 10px;
                height: 10px;
                background: #00ff88;
                border-radius: 50%;
                display: inline-block;
                animation: pulse 2s infinite;
            }
            @keyframes pulse {
                0%, 100% { opacity: 1; transform: scale(1); }
                50% { opacity: 0.5; transform: scale(1.2); }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        {%config%}
        {%scripts%}
        {%renderer%}
    </body>
</html>
'''


DB_PATH = 'data/warehouse/social_analytics.db'

def get_data():
    if not os.path.exists(DB_PATH):
        return create_fallback_data()
    
    try:
        con = duckdb.connect(DB_PATH)
        
        events_df = con.execute("""
            SELECT event_type, COUNT(*) as count 
            FROM fact_events GROUP BY 1 ORDER BY 2 DESC
        """).df()
        
        regions_df = con.execute("""
            SELECT u.region, COUNT(f.event_id) as activity
            FROM fact_events f
            JOIN dim_users u ON f.user_id = u.user_id
            GROUP BY 1 ORDER BY 2 DESC
        """).df()
        
        time_df = con.execute("""
            SELECT DATE(event_time) as date, COUNT(*) as events
            FROM fact_events
            GROUP BY 1 ORDER BY 1
        """).df()
        
        platform_df = con.execute("""
            SELECT u.platform, COUNT(f.event_id) as events
            FROM fact_events f
            JOIN dim_users u ON f.user_id = u.user_id
            GROUP BY 1 ORDER BY 2 DESC
        """).df()
        
        user_activity_df = con.execute("""
            SELECT u.user_id, u.region, u.platform, COUNT(f.event_id) as events
            FROM fact_events f
            JOIN dim_users u ON f.user_id = u.user_id
            GROUP BY 1, 2, 3
            ORDER BY 4 DESC
            LIMIT 20
        """).df()
        
        con.close()
        return events_df, regions_df, time_df, platform_df, user_activity_df
    except Exception as e:
        print(f"Data fetch error: {e}")
        return create_fallback_data()

def create_fallback_data():
    return (
        pd.DataFrame({'event_type': ['post', 'like', 'comment', 'share'], 'count': [1250, 980, 450, 280]}),
        pd.DataFrame({'region': ['North America', 'Europe', 'Asia Pacific', 'Latin America'], 'activity': [850, 620, 480, 150]}),
        pd.DataFrame({'date': pd.date_range('2026-04-20', periods=10), 'events': [120, 145, 98, 167, 203, 178, 156, 189, 221, 198]}),
        pd.DataFrame({'platform': ['Twitter', 'Instagram', 'LinkedIn', 'Facebook'], 'events': [680, 520, 340, 160]}),
        pd.DataFrame({'user_id': list(range(1, 11)), 'region': ['NA', 'EU', 'APAC', 'NA', 'EU', 'APAC', 'NA', 'EU', 'APAC', 'NA'], 'platform': ['Twitter']*10, 'events': [50, 45, 40, 35, 30, 28, 25, 22, 20, 18]})
    )


CHART_COLORS = ['#00f2ff', '#7000ff', '#ff007a', '#00ff88', '#ffaa00', '#ff6b6b']

def create_event_chart(df):
    fig = px.bar(
        df, x='event_type', y='count',
        text='count',
        color='count',
        color_continuous_scale=['#7000ff', '#00f2ff'],
    )
    fig.update_traces(
        textposition='outside',
        marker=dict(line=dict(color='#00f2ff', width=2), line_width=2),
        hovertemplate="<b>%{x}</b><br>Count: %{y:,}<extra></extra>"
    )
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', color='#aaa'),
        margin=dict(t=50, r=20, b=40, l=30),
        xaxis=dict(showgrid=False, title=''),
        yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', title=''),
        showlegend=False,
        coloraxis_showscale=False,
    )
    return fig

def create_region_chart(df):
    fig = px.pie(
        df, names='region', values='activity',
        hole=0.65,
        color_discrete_sequence=CHART_COLORS,
    )
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        marker=dict(line=dict(color='#0a0a0f', width=3)),
        hovertemplate="<b>%{label}</b><br>Activity: %{value:,}<br>Share: %{percent}<extra></extra>"
    )
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', color='#aaa'),
        margin=dict(t=50, r=20, b=40, l=20),
    )
    return fig

def create_time_chart(df):
    fig = px.area(
        df, x='date', y='events',
        color_discrete_sequence=['#00f2ff'],
    )
    fig.update_traces(
        fill='tozeroy',
        fillcolor='rgba(0, 242, 255, 0.15)',
        line=dict(width=3),
        hovertemplate="<b>%{x}</b><br>Events: %{y:,}<extra></extra>"
    )
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', color='#aaa'),
        margin=dict(t=50, r=20, b=40, l=30),
        xaxis=dict(showgrid=False, title=''),
        yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', title=''),
    )
    return fig

def create_platform_chart(df):
    fig = px.bar(
        df, y='platform', x='events',
        orientation='h',
        text='events',
        color='events',
        color_continuous_scale=['#7000ff', '#ff007a'],
    )
    fig.update_traces(
        textposition='outside',
        marker=dict(line=dict(color='#ff007a', width=2)),
        hovertemplate="<b>%{y}</b><br>Events: %{x:,}<extra></extra>"
    )
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', color='#aaa'),
        margin=dict(t=50, r=20, b=40, l=60),
        xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', title=''),
        yaxis=dict(title=''),
        showlegend=False,
        coloraxis_showscale=False,
    )
    return fig


df_events, df_regions, df_time, df_platform, df_users = get_data()

total_events = df_events['count'].sum()
top_event = df_events.iloc[0]['event_type'].upper() if len(df_events) > 0 else "N/A"
active_regions = len(df_regions)
top_platform = df_platform.iloc[0]['platform'].upper() if len(df_platform) > 0 else "N/A"


app.layout = dash.html.Div([
    dash.html.Div([
        dash.html.Div([
            dash.html.H1("🔗 NETWORK INTELLIGENCE", className="header-title mb-2"),
            dash.html.Div([
                dash.html.Span(className="pulse-dot me-2"),
                dash.html.Span("LIVE", className="me-3", style={'fontSize': '12px', 'color': '#00ff88'}),
                dash.html.Span(id="clock", style={'fontFamily': 'Poppins', 'fontSize': '13px', 'color': '#666'}),
            ]),
        ], className="text-center py-4"),
    ], className="mb-4"),
    
    dash.html.Div([
        dash.html.Button(
            [dash.html.Span("⟳ "), " Refresh Data"],
            id="refresh-btn",
            className="btn-refresh text-white",
        ),
        dash.html.Div(id="loading-indicator", style={'display': 'none'}),
    ], className="text-center mb-4"),
    
    dash.html.Div([
        dash.html.Div([
            dash.html.Div("TOTAL EVENTS", style={'fontSize': '11px', 'color': '#666', 'letterSpacing': '1px'}),
            dash.html.Div(f"{total_events:,}", className="kpi-value", style={'color': '#00f2ff'}),
            dash.html.Div("processed", style={'fontSize': '11px', 'color': '#555'}),
        ], className="card-custom kpi-card p-4", style={'borderColor': '#00f2ff'}),
        
        dash.html.Div([
            dash.html.Div("TOP EVENT", style={'fontSize': '11px', 'color': '#666', 'letterSpacing': '1px'}),
            dash.html.Div(top_event, className="kpi-value", style={'fontSize': '2rem', 'color': '#7000ff'}),
            dash.html.Div("most common", style={'fontSize': '11px', 'color': '#555'}),
        ], className="card-custom kpi-card p-4", style={'borderColor': '#7000ff'}),
        
        dash.html.Div([
            dash.html.Div("REGIONS", style={'fontSize': '11px', 'color': '#666', 'letterSpacing': '1px'}),
            dash.html.Div(str(active_regions), className="kpi-value", style={'color': '#ff007a'}),
            dash.html.Div("active areas", style={'fontSize': '11px', 'color': '#555'}),
        ], className="card-custom kpi-card p-4", style={'borderColor': '#ff007a'}),
        
        dash.html.Div([
            dash.html.Div("TOP PLATFORM", style={'fontSize': '11px', 'color': '#666', 'letterSpacing': '1px'}),
            dash.html.Div(top_platform, className="kpi-value", style={'fontSize': '1.8rem', 'color': '#00ff88'}),
            dash.html.Div("highest engagement", style={'fontSize': '11px', 'color': '#555'}),
        ], className="card-custom kpi-card p-4", style={'borderColor': '#00ff88'}),
    ], className="row g-4 mb-4"),
    
    dash.html.Div([
        dash.html.Div([
            dash.html.Div([
                dash.html.H4("📊 Event Distribution", style={'color': '#888', 'fontSize': '14px', 'marginBottom': '15px'}),
                dcc.Graph(id='events-chart', figure=create_event_chart(df_events), style={'height': '320px'}),
            ], className="card-custom p-3"),
        ], className="col-md-6 mb-4"),
        
        dash.html.Div([
            dash.html.Div([
                dash.html.H4("🌍 Regional Activity", style={'color': '#888', 'fontSize': '14px', 'marginBottom': '15px'}),
                dcc.Graph(id='regions-chart', figure=create_region_chart(df_regions), style={'height': '320px'}),
            ], className="card-custom p-3"),
        ], className="col-md-6 mb-4"),
    ], className="row"),
    
    dash.html.Div([
        dash.html.Div([
            dash.html.Div([
                dash.html.H4("📈 Activity Timeline", style={'color': '#888', 'fontSize': '14px', 'marginBottom': '15px'}),
                dcc.Graph(id='time-chart', figure=create_time_chart(df_time), style={'height': '280px'}),
            ], className="card-custom p-3"),
        ], className="col-md-8 mb-4"),
        
        dash.html.Div([
            dash.html.Div([
                dash.html.H4("📱 Platform Stats", style={'color': '#888', 'fontSize': '14px', 'marginBottom': '15px'}),
                dcc.Graph(id='platform-chart', figure=create_platform_chart(df_platform), style={'height': '280px'}),
            ], className="card-custom p-3"),
        ], className="col-md-4 mb-4"),
    ], className="row"),
    
    dash.html.Div([
        dash.html.P("Network Intelligence Dashboard v2.1 • Real-time Analytics", 
                   style={'color': '#444', 'fontSize': '12px', 'textAlign': 'center'}),
    ], className="mt-4 pb-4"),
    
    dcc.Store(id='data-store', data={
        'events': df_events.to_dict('records'),
        'regions': df_regions.to_dict('records'),
        'time': df_time.to_dict('records'),
        'platform': df_platform.to_dict('records'),
    }),
    
], style={'padding': '20px', 'maxWidth': '1400px', 'margin': '0 auto'})


@callback(
    Output('clock', 'children'),
    Input('refresh-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def update_clock(n):
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@callback(
    [Output('events-chart', 'figure'),
     Output('regions-chart', 'figure'),
     Output('time-chart', 'figure'),
     Output('platform-chart', 'figure')],
    Input('refresh-btn', 'n_clicks'),
    prevent_initial_call=True,
)
def refresh_charts(n):
    df_events, df_regions, df_time, df_platform, _ = get_data()
    return (
        create_event_chart(df_events),
        create_region_chart(df_regions),
        create_time_chart(df_time),
        create_platform_chart(df_platform),
    )


if __name__ == "__main__":
    print("🚀 Starting Dashboard at http://localhost:8050")
    app.run(debug=True, port=8050)