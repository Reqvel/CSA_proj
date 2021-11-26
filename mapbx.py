import pip._vendor.requests as requests
import json

class MapBx:
    mapbox_access_token = None

    def __init__(self, access_token_file: str, access_token_key: str) -> None:
        self.access_token_file = access_token_file
        self.access_token_key = access_token_key
        self.set_token()
        

    def set_token(self):
        with open(self.access_token_file, 'r') as openfile:
            json_object = json.load(openfile)
        self.mapbox_access_token = json_object[self.access_token_key]


    def get_location_center(self, location_name: str, info: bool) -> tuple:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{location_name}.json?country=by&access_token={self.mapbox_access_token}"
        json_data = requests.get(url).json()
        if(json_data and json_data['features']):
            for res in json_data['features']:
                if(res['text'] == location_name):
                    if(info):
                        print(f"place_name : {res['place_name']}")
                        print(f"text : {res['text']}")
                        print(f"center : {res['center']}")
                        print('\n\n')
                    longitude = res['center'][0]
                    latitude = res['center'][1]
                    return longitude, latitude
        else:
            if(info):
                print("*** DATA IS NULL ***")
                print('\n\n')
        return None, None