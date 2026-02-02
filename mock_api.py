from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

COMMENTS = [
    {"id": "1", "text": "Hallo, wie geht das?", "replies": []},
    {"id": "2", "text": "Nice!", "replies": []},
]

class ReplyIn(BaseModel):
    text: str

def check_auth(authorization: Optional[str]):
    if authorization != "Bearer TESTTOKEN":
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/api/comments")
def list_comments(authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    return {"items": COMMENTS}

@app.post("/api/comments/{comment_id}/reply")
def reply(comment_id: str, payload: ReplyIn, authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    for c in COMMENTS:
        if c["id"] == comment_id:
            c["replies"].append(payload.text)
            return {"status": "ok", "comment_id": comment_id, "reply_count": len(c["replies"])}
    raise HTTPException(status_code=404, detail="Comment not found")

@app.post("/api/test/add_comment")
def add_comment(text: str, authorization: Optional[str] = Header(default=None)):
    check_auth(authorization)
    new_id = str(int(COMMENTS[-1]["id"]) + 1) if COMMENTS else "1"
    COMMENTS.append({"id": new_id, "text": text, "replies": []})
    return {"status": "ok", "id": new_id}
