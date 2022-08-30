from typing import Any, List, Optional
import uuid

from fastapi.params import Query, Path
from datasources import ClientsDataSource

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import *

import schemas
import deps

router = APIRouter()


# All or multiple clients
@router.get("/", response_model=List[schemas.Client])
def read_clients(
    clients_datasource: ClientsDataSource = Depends(deps.get_clients_datasource),
    clients_ids: Optional[List[uuid.UUID]] = Query([], alias="clientId[]"),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
) -> Any:
    if clients_ids:
        clients = clients_datasource.get_multiple_clients(clients_ids, skip, limit)
    else:
        clients = clients_datasource.get_all_clients(skip, limit)

    if clients.empty:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"None of the clients exist.",
        )

    return clients.to_dict(orient="records")


# Single client
@router.get("/{client_id}", response_model=schemas.Client)
def read_clients(
    clients_datasource: ClientsDataSource = Depends(deps.get_clients_datasource),
    client_id: uuid.UUID = Path(...),
) -> Any:
    client = clients_datasource.get_single_client(client_id)

    if not client:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Client with id {client_id} doesn't exist.",
        )

    return client
