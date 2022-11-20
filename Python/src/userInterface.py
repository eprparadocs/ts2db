"""

This is the top of the TS2DB 
"""
from fastapi import FastAPI

app = FastAPI()


@app.get("/login")
def login():
    """
    Get access to the microservice.
    """
    return {"Hello": "login"}

@app.get("/logout")
def logout():
    """
    Logout of the service.
    """
    return {"Hello": "logout"}

@app.post("/graphQL")
def graphQL_request(gqlreq):
    """
    Start all GraphQL operations here.
    """
    return {"Hello": "graphQL"}

@app.post("/sql")
def sql_request(sqlreq):
    """
    Handle all SQL requests here!
    """
    return{"Hello": "sql"}
