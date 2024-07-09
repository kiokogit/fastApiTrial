from typing import Annotated

from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from fastapi import APIRouter, Depends, HTTPException, status

from crm.schemas.collections import BaseCollectionSchema, CollectionSchema, CollectionPatchSchema, \
                                    EntryCreateSchema, EntrySchema
from crm.dependencies import MongoDb

router = APIRouter()


allowed_collections = [
    'filters',
    'startup_titles',
    'competitors_titles',
]


def load_collection(db: MongoDb, name: str):
    if name not in allowed_collections:
        raise ValueError(f"Collection '{name}' not allowed")

    if (collection := db.get_collection(name)) is not None:
        return collection

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Collection with ID {name} not found")


Collection = Annotated[Collection, Depends(load_collection)]


@router.get('')
def list_collections() -> list[BaseCollectionSchema]:
    return [{'name': n} for n in allowed_collections]


@router.get('/{name}')
def get_collection(name: str, coll: Collection) -> CollectionSchema:
    return dict(name=name, items=list(coll.find(limit=100)))


@router.patch('/{name}')
def patch_collection(patch: CollectionPatchSchema, coll: Collection) -> CollectionSchema:
    for item in (patch.patch_items or []):
        try:
            coll.update_one({"_id": item.id}, {"$set": item.dict(exclude_unset=True)})
        except DuplicateKeyError:
            raise HTTPException(status_code=409, detail=f"Item {item.value} already exists, creating duplicates is prohibited")

    for item_id in (patch.delete_items or []):
        coll.delete_one({"_id": item_id})

    return dict(name=coll.name, items=list(coll.find(limit=100)))


@router.put('/{name}/items')
def add_item(item: EntryCreateSchema, coll: Collection) -> EntrySchema:
    try:
        new_item = coll.insert_one(item.dict())
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail=f"Item with value \
                '{item.value}' already exists, creating duplicates is prohibited")

    return coll.find_one({"_id": new_item.inserted_id})
