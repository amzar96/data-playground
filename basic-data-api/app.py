import uuid
from database import database as db
from fastapi import FastAPI, HTTPException

app = FastAPI()


def get_actual_id(id):
    print(db.getBy({"id": id}))
    return db.getBy({"id": int(id)})[0]["id"]


@app.post("/items/")
def create_item(name: str, description: str):
    new_id = db.add({"name": name, "description": description})
    return {"id": new_id, "name": name, "description": description}


@app.get("/items/")
def read_items():
    items = db.getAll()
    return {"items": items}


@app.get("/items/{item_id}")
def read_item(item_id):
    item = db.getById(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@app.put("/items/{item_id}")
def update_item(item_id: int, name: str, description: str):
    item_id = get_actual_id(item_id)

    updated = db.updateById(item_id, {"name": name, "description": description})
    if not updated:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"id": item_id, "name": name, "description": description}


@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    item_id = get_actual_id(item_id)

    deleted = db.deleteById(item_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"message": "Item deleted successfully"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
