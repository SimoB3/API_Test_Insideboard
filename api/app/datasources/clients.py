from typing import List, Optional
from pathlib import Path
import uuid

import pandas as pd
import schemas


class ClientsDataSource:
    all_clients = None

    def __init__(self, filepath: Path) -> None:
        if not self.all_clients:
            self.all_clients = pd.read_csv(filepath)

    def get_all_clients(self, skip: int, limit: int) -> pd.DataFrame:
        return self.all_clients.iloc[skip : skip + limit]

    def get_multiple_clients(self, client_ids: List[uuid.UUID], skip: int, limit: int) -> pd.DataFrame:
        return self.all_clients.loc[self.all_clients["id"].isin(map(str, client_ids))].iloc[skip : skip + limit]

    def get_single_client(self, client_id: uuid.UUID) -> Optional[schemas.Client]:
        client = self.all_clients.loc[self.all_clients["id"] == str(client_id)]
        return schemas.Client(**client.iloc[0]) if len(client) else None
