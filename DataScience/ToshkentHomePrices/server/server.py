from flask import Flask, request, jsonify
from utilization import locations, predict_price

app = Flask(__name__)


# @app.route('/hello')
# def hello():
#     print('hello')
#     response = 'hello2233'
#     return response


@app.route('/get_location_name')
def get_location_name():
    print(locations)
    response = jsonify({
        'locations': locations
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@app.route('/predict_home_price', methods=['GET', 'POST'])
def predict_home_price():
    print(request)
    area = float(request.form['Area'])
    rooms = int(request.form['rooms'])
    district = str(request.form['district'])
    type = str(request.form['type'])

    response = jsonify({
        'estimated_price': predict_price(area, rooms, district, type)
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    print("Starting Python Flask Server For Home Price Prediction...")
    app.run()
    #host='localhost', port='8000', debug=True
