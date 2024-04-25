from pydantic import BaseModel, Field, field_validator

from dwh.core.domain.entities.gender import Gender


class Passenger(BaseModel):
    age: float = Field(alias="Age")
    fare: float = Field(alias="Fare")
    name: str = Field(alias="Name")
    p_class: int = Field(alias="Pclass")
    parents_children_aboard: int = Field(alias="Parents/Children Aboard")
    siblings_spouses_aboard: int = Field(alias="Siblings/Spouses Aboard")
    gender: Gender = Field(alias="Sex")
    survived: bool = Field(alias="Survived")

    @field_validator("gender", mode="before")
    @classmethod
    def decode_gender(cls, obj: str) -> Gender:
        if obj == "male":
            return Gender.MALE

        if obj == "female":
            return Gender.FEMALE

        raise ValueError(f"sex {obj} is not recognized.")
