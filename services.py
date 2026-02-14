from notif_config import settings
import httpx
from fastapi import HTTPException


class Service:
    def __init__(self):
        self._base_url = ""

    def _build_base_url(
        self,
        host: str = None,
        port: str = None,
        endpoint: str = None,
        param: str = None
    ):
        url = "http://"
        if host is not None:
            url += host
        if port is not None:
            url += ':' + port
        if endpoint is not None:
            url += '/' + endpoint
        if param is not None:
            url += '/' + param
        print(url)
        self._base_url = url

    def _check_response(
        self,
        response
    ):
        if not response.is_success:
            raise HTTPException(
                status_code=response.status_code, 
                detail=response.text
            )
        

    def _build_endpoint_url(
        self,
        endpoint: str = None,
        param: str = None
    ):
        url = self._base_url
        if endpoint is not None:
            url += '/' + endpoint
        if param is not None:
            url += '/' + param
        print(url)
        return url


class ProfileService(Service):
    def __init__(self):
        self._build_base_url(
            host = settings.prof_url.host,
            port = settings.prof_url.port 
        )


    async def get_profile(
        self,
        username: str
    ):
        url = self._build_endpoint_url(settings.prof_url.get_endpoint, username)

        async with httpx.AsyncClient() as client:
            response = await client.get(url)

        self._check_response(response)

        return response