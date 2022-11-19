from fastapi import FastAPI

app = FastAPI()

userI
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
def graphQL_request():
    """
    Start all GraphQL operations here.
    """
    return {"Hello": "graphQL"}

@app.post("/sql")
def sql_request():
    """
    Handle all SQL requests here!
    """
    return("Hello": "sql")
