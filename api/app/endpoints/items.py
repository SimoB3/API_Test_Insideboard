from typing import Any, List, Optional
import uuid

from fastapi.params import Query, Path
from datasources import ItemsDataSource

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import *

import schemas
import deps

router = APIRouter()


# All or multiple items
@router.get("/", response_model=List[schemas.Item])
def read_items(
    items_datasource: ItemsDataSource = Depends(deps.get_items_datasource),
    items_ids: Optional[List[uuid.UUID]] = Query([], alias="itemId[]"),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
) -> Any:
    if items_ids:
        items = items_datasource.get_multiple_items(items_ids, skip, limit)
    else:
        items = items_datasource.get_all_items(skip, limit)

    if items.empty:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"None of the items exist.",
        )

    return items.to_dict(orient="records")


# Single item
@router.get("/{item_id}", response_model=schemas.Item)
def read_items(
    items_datasource: ItemsDataSource = Depends(deps.get_items_datasource),
    item_id: uuid.UUID = Path(...),
) -> Any:
    item = items_datasource.get_single_item(item_id)

    if not item:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Item with id {item_id} doesn't exist.",
        )

    return item
