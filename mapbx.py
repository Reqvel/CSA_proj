import json
import requests
import time

class MapBx:
    _mapbox_access_token = None

    def __init__(self, access_token_file: str, access_token_key: str) -> None:
        self.access_token_file = access_token_file
        self.access_token_key = access_token_key
        self.set_token()
        

    def set_token(self):
        with open(self.access_token_file, 'r') as openfile:
            json_object = json.load(openfile)
        self._mapbox_access_token = json_object[self.access_token_key]


    async def get_location_center_async(self, session, location_name: str, id: int) -> tuple:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{location_name}.json?country=by&access_token={self._mapbox_access_token}"
        
        async with session.get(url) as response:
            json_data = await response.json()
            if(json_data and json_data['features']):
                for res in json_data['features']:
                    if(res['text'] == location_name):
                        longitude = res['center'][0]
                        latitude = res['center'][1]
                        return id, longitude, latitude
            return id, None, None


    def get_location_center(self, location_name: str, id: int) -> tuple:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{location_name}.json?country=by&access_token={self._mapbox_access_token}"
        
        json_data = json_data = requests.get(url).json()
        if(json_data and json_data['features']):
            for res in json_data['features']:
                if(res['text'] == location_name):
                    longitude = res['center'][0]
                    latitude = res['center'][1]
                    print(f'id: {id}, lon: {longitude}, lat: {latitude}')
                    return id, longitude, latitude
        else: print(f'\n{json_data}')
        return id, None, None


    async def get_duration_async(
        self,
        session,
        a_id: int, 
        a_lon: float,
        a_lat: float,
        b_id: int, 
        b_lon: float,
        b_lat: float) -> tuple:

        url = f"https://api.mapbox.com/optimized-trips/v1/mapbox/driving/{a_lon},{a_lat};{b_lon},{b_lat}?access_token={self._mapbox_access_token}"

        async with session.get(url) as response:
            json_data = await response.json()
            if('code' in json_data and json_data['code'] == 'Ok'):
                duration = json_data['trips'][0]['duration']
                return a_id, b_id, duration
            else: print(f'\n{json_data}')
        
        return a_id, b_id, None


    def get_duration(
        self,
        a_id: int, 
        a_lon: float,
        a_lat: float,
        b_id: int, 
        b_lon: float,
        b_lat: float) -> tuple:

        url = f"https://api.mapbox.com/optimized-trips/v1/mapbox/driving/{a_lon},{a_lat};{b_lon},{b_lat}?access_token={self._mapbox_access_token}"

        for _ in range(4):
            json_data = json_data = requests.get(url).json()
            if('code' in json_data and json_data['code'] == 'Ok'):
                duration = json_data['trips'][0]['duration']
                print(f'a_id: {a_id}, b_id: {b_id}, dur: {duration}')
                return a_id, b_id, duration
            else: 
                print(f'\n{json_data}. *** Trying again ***')
                time.sleep(15)
        
        print('\n*** Attempts failed! ***')
        return a_id, b_id, None


    def get_token(self):
        return self._mapbox_access_token
