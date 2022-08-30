from fastapi import FastAPI

import endpoints

api = FastAPI()
api.include_router(endpoints.clients.router, prefix="/clients", tags=["clients"])
api.include_router(endpoints.items.router, prefix="/items", tags=["items"])
