from pydantic import BaseModel, Field

from dwh.core.domain.entities.gender import Gender


class Passenger(BaseModel):
    age: float = Field()
    fare: float = Field()
    name: str = Field()
    p_class: int = Field()
    parents_children_aboard: int = Field()
    gender: Gender = Field()
    siblings_spouses_aboard: int = Field()
    survived: bool = Field()
