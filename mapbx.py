import asyncio
import aiohttp
import pip._vendor.requests as requests
import json

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


    async def get_location_center(self, session, location_name: str) -> tuple:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{location_name}.json?country=by&access_token={self._mapbox_access_token}"
        
        async with session.get(url) as response:
            json_data = await response.json()
            if(json_data and json_data['features']):
                for res in json_data['features']:
                    if(res['text'] == location_name):
                        longitude = res['center'][0]
                        latitude = res['center'][1]
                        return longitude, latitude
            return None, None


    def get_token(self):
        return self._mapbox_access_token
