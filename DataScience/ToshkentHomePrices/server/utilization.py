import json
import pickle
import numpy as np

data_columns = json.load(open("/home/ubuntu/Olx_THP_org/server/artifacts/columns_new.json", "r"))['data_columns']
locations = data_columns[3:]
model1 = pickle.load(
    open("/home/ubuntu/Olx_THP_org/server/artifacts/tashkent_new_home_prices_model.pickle", "rb"))
model2 = pickle.load(
    open("/home/ubuntu/Olx_THP_org/server/artifacts/tashkent_secondary_home_prices_model.pickle", "rb"))


def predict_price(area, rooms, district, type):

    if type.lower() == 'new':
        try:
            loc_index = data_columns.index(district.lower())

        except:
            loc_index = -1

        x = np.zeros(len(data_columns))
        x[0] = area
        x[1] = rooms
        if loc_index >= 0:
            x[loc_index] = 1

        return round(model1.predict([x])[0], 2)

    if type.lower() == 'secondary':
        try:
            loc_index = data_columns.index(district.lower())

        except:
            loc_index = -1

        x = np.zeros(len(data_columns))
        x[0] = area
        x[1] = rooms
        if loc_index >= 0:
            x[loc_index] = 1

        return round(model2.predict([x])[0], 2)
