import requests

base_url_mapping = {"data.gov": "https://api.data.gov.my/data-catalogue/"}


class API:
    def __init__(self, _type):
        self.base_url = base_url_mapping.get(_type, None)

        if not self.base_url:
            raise KeyError(f"{_type} is not found in base URL mapping")

    def get_full_url(self, endpoint: str, params: dict = None) -> str:
        url = f"{self.base_url}{endpoint}"
        if params:
            query_string = "&".join(f"{key}={value}" for key, value in params.items())
            url = f"{url}?{query_string}"
        return url

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: dict = None,
        data: dict = None,
        headers: dict = None,
    ) -> dict:
        self.url = self.get_full_url(endpoint, params)
        method = method.upper()

        try:
            if method == "GET":
                response = requests.get(self.url, headers=headers)
            elif method == "POST":
                response = requests.post(self.url, json=data, headers=headers)
            else:
                raise ValueError("invalid HTTP method")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)} 
