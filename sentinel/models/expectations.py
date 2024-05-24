from typing import Dict, Optional

from pydantic import BaseModel


class Expectations(BaseModel):

    expectation_type: Optional[str] = None
    expectation_type: Optional[str] = None
    kwargs: Optional[Dict[str, any]] = None

    class Config:
        arbitrary_types_allowed = True
