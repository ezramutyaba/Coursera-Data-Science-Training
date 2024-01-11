import pandas as pd
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import plotly.express as px
from dash import no_update

app = dash.Dash(__name__)

# REVIEW1: Clear the layout and do not display exception till callback gets executed
app.config.suppress_callback_exceptions = True

# Read the automobiles data into pandas dataframe
auto_data =  pd.read_csv('automobileEDA.csv', 
                            encoding = "ISO-8859-1",
                            )

#Layout Section of Dash

app.layout = html.Div(children=[
    html.H1('Car Automobile Components', 
                                style={'textAlign': 'center', 'color': '#503D36',
                                'font-size': 24}),

     #outer division starts
     html.Div([
                   # First inner divsion for  adding dropdown helper text for Selected Drive wheels
                    html.Div(
                            #TASK 3B
                     ),
                    

                    #TASK 3C

                    #Second Inner division for adding 2 inner divisions for 2 output graphs 
                    html.Div([
                
                        #TASK 3D
                        
                    ], style={'display': 'flex'}),

    ])
    #outer division ends

])
#layout ends

#Place to add @app.callback Decorator
#TASK 3E

   
#Place to define the callback function .
#TASK 3F
   
   

if __name__ == '__main__':
    app.run_server()
    