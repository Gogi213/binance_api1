from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:19938713@localhost:3306/binance_project'
db = SQLAlchemy(app)

class BinanceData(db.Model):
    __tablename__ = 'binance_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(50))
    status = db.Column(db.String(50))
    pair = db.Column(db.String(50))
    base = db.Column(db.String(50))
    quote = db.Column(db.String(50))
    bidprice = db.Column(db.String(50))
    bidqty = db.Column(db.String(50))
    askprice = db.Column(db.String(50))
    askqty = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    amount = db.Column(db.String(50))
    usdt_equals = db.Column(db.String(50))
    profit = db.Column(db.String(50))
    pair2 = db.Column(db.String(50))
    base2 = db.Column(db.String(50))
    quote2 = db.Column(db.String(50))
    bidprice2 = db.Column(db.String(50))
    bidqty2 = db.Column(db.String(50))
    askprice2 = db.Column(db.String(50))
    askqty2 = db.Column(db.String(50))
    swap2 = db.Column(db.String(50))
    amount2 = db.Column(db.String(50))
    usdt_equals2 = db.Column(db.String(50))
    profit2 = db.Column(db.String(50))
    pair3 = db.Column(db.String(50))
    base3 = db.Column(db.String(50))
    quote3 = db.Column(db.String(50))
    bidprice3 = db.Column(db.String(50))
    bidqty3 = db.Column(db.String(50))
    askprice3 = db.Column(db.String(50))
    askqty3 = db.Column(db.String(50))
    swap3 = db.Column(db.String(50))
    amount3 = db.Column(db.String(50))
    usdt_equals3 = db.Column(db.String(50))
    profit3 = db.Column(db.String(50))

@app.route('/')
def home():
    table1 = db.session.query(BinanceData).all()
    return render_template('home.html', table1=table1)

if __name__ == '__main__':
    app.run(debug=True)