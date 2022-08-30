from typing import List, Optional
from pathlib import Path
import uuid

import pandas as pd
import schemas


class ItemsDataSource:
    all_items = None

    def __init__(self, filepath: Path) -> None:
        if not self.all_items:
            self.all_items = pd.read_csv(filepath)

    def get_all_items(self, skip: int, limit: int) -> pd.DataFrame:
        return self.all_items.iloc[skip : skip + limit]

    def get_multiple_items(self, item_ids: List[uuid.UUID], skip: int, limit: int) -> pd.DataFrame:
        return self.all_items.loc[self.all_items["id"].isin(map(str, item_ids))].iloc[skip : skip + limit]

    def get_single_item(self, item_id: uuid.UUID) -> Optional[schemas.Item]:
        item = self.all_items.loc[self.all_items["id"] == str(item_id)]
        return schemas.Item(**item.iloc[0]) if len(item) else None
