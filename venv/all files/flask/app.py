from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
import pandas as pd

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:19938713@localhost:3306/binance_project'
db = SQLAlchemy(app)

@app.route('/')
def home():
    table1 = pd.read_sql('SELECT * FROM binance_data', db.session.bind)
    table2 = pd.read_sql('SELECT * FROM binance_prices', db.session.bind)
    return render_template('home.html', tables=[table1.to_html(classes='data'), table2.to_html(classes='data')], titles=['na', 'Table 1', 'Table 2'])

if __name__ == '__main__':
    app.run(debug=True)
